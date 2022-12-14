package s3fs

import (
	"errors"
	"io/fs"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
)

type Fs struct {
	s3     *s3.S3
	bucket string
}

func NewFS(bucket string, s3 *s3.S3) *Fs {
	return &Fs{bucket: bucket, s3: s3}
}

// Open a file for reading.
func (s3fs *Fs) Open(name string) (fs.File, error) {
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
func (s3fs Fs) Stat(name string) (fs.FileInfo, error) {
	resp, err := s3fs.s3.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(s3fs.bucket),
		Key:    aws.String(name),
	})
	if err != nil {
		var awsErr awserr.RequestFailure
		if errors.As(err, &awsErr) {
			if awsErr.StatusCode() == 404 {
				// its not a file, see if its a directory
				return s3fs.statDirectory(name)
			}
		}
		return nil, &fs.PathError{
			Op:   "stat",
			Path: name,
			Err:  err,
		}
	}
	if strings.HasSuffix(name, "/") {
		// accept invisible directories as directories
		return newDirInfo(name), nil
	}
	return newFileInfo(name, *resp.ContentLength, *resp.LastModified), nil
}

func (s3fs Fs) statDirectory(name string) (fs.FileInfo, error) {
	name = path.Clean(name)
	resp, err := s3fs.s3.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket:  aws.String(s3fs.bucket),
		Prefix:  aws.String(strings.TrimPrefix(name, "/")),
		MaxKeys: aws.Int64(1),
	})
	if err != nil {
		return nil, &fs.PathError{Op: "stat", Path: name, Err: err}
	}
	if *resp.KeyCount == 0 && name != "" {
		return nil, &fs.PathError{Op: "stat", Path: name, Err: fs.ErrNotExist}
	}
	return newDirInfo(name), nil
}
