package s3fs

import (
	"context"
	"errors"
	"io/fs"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"go.uber.org/zap"
)

// S3FS is an FS object backed by S3.
type S3FS struct {
	s3     *s3.S3
	bucket string // Bucket name
	log    *zap.Logger
}

// NewFs creates a new Fs object writing files to a given S3 bucket.
func NewFS(bucket string, s3 *s3.S3, log *zap.Logger) *S3FS {
	return &S3FS{
		bucket: bucket,
		s3:     s3,
		log:    log,
	}
}

// Name returns the type of FS object this is: Fs.
func (S3FS) Name() string { return "s3" }

// Open a file for reading.
func (s3fs *S3FS) Open(name string) (fs.File, error) {
	file := newFile(s3fs, name)

	info, err := file.Stat()
	if err != nil {
		return nil, err
	}

	if info.IsDir() {
		return file, nil
	}

	return file, nil
}

// Stat returns a FileInfo describing the named file.
// If there is an error, it will be of type *os.PathError.
func (s3fs S3FS) Stat(name string) (fs.FileInfo, error) {
	resp, err := s3fs.s3.HeadObjectWithContext(context.TODO(), &s3.HeadObjectInput{
		Bucket: aws.String(s3fs.bucket),
		Key:    aws.String(name),
	})
	if err != nil {
		var awsErr awserr.RequestFailure
		if errors.As(err, &awsErr) && awsErr.StatusCode() == 404 {
			statDir, errStat := s3fs.statDirectory(name)
			return statDir, errStat
		}
		return nil, &fs.PathError{
			Op:   "stat",
			Path: name,
			Err:  err,
		}
	}

	if strings.HasSuffix(name, "/") {
		// accept invisible directories as directories
		return newDirEntry(name), nil
	}

	return newFileInfo(path.Base(name), *resp.ContentLength, *resp.LastModified), nil
}

func (s3fs S3FS) statDirectory(name string) (fs.FileInfo, error) {
	name = path.Clean(name)
	resp, err := s3fs.s3.ListObjectsV2WithContext(context.TODO(), &s3.ListObjectsV2Input{
		Bucket:  aws.String(s3fs.bucket),
		Prefix:  aws.String(strings.TrimPrefix(name, "/")),
		MaxKeys: aws.Int64(1),
	})
	if err != nil {
		return nil, &fs.PathError{
			Op:   "stat",
			Path: name,
			Err:  err,
		}
	}
	if *resp.KeyCount == 0 && name != "" {
		return nil, &fs.PathError{
			Op:   "stat",
			Path: name,
			Err:  fs.ErrNotExist,
		}
	}
	return newDirEntry(path.Base(name)), nil
}
