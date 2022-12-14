package s3fs

import (
	"errors"
	"io"
	"io/fs"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

// s3File represents a file in S3.
type s3File struct {
	info fs.FileInfo // File info cached for later used

	fs   *Fs    // Parent file system
	name string // Name of the file

	readDirContToken    *string // readdirContinuationToken is used to perform files listing across calls
	readDirNotTruncated bool    // readdirNotTruncated is set when we shall continue reading

	offset int64 // current offset of the read stream

	in io.ReadCloser // streamRead is the underlying stream we are reading from
}

const READAHEAD = 1024 * 64 // 64kb readahead

func newFile(fs *Fs, name string) *s3File {
	return &s3File{
		fs:   fs,
		name: name,
	}
}

func (f *s3File) Name() string {
	return f.name
}

func (f *s3File) ReadDir(n int) ([]fs.DirEntry, error) {
	if f.readDirNotTruncated {
		return nil, io.EOF
	}
	if n <= 0 {
		return f.ReadDirAll()
	}
	// ListObjects treats leading slashes as part of the directory name
	// It also needs a trailing slash to list contents of a directory.
	name := strings.TrimPrefix(f.Name(), "/") // + "/"

	// For the root of the bucket, we need to remove any prefix
	if name != "" && !strings.HasSuffix(name, "/") {
		name = name + "/"
	}

	resp, err := f.fs.s3.ListObjectsV2(&s3.ListObjectsV2Input{
		ContinuationToken: f.readDirContToken,
		Bucket:            aws.String(f.fs.bucket),
		Prefix:            aws.String(name),
		Delimiter:         aws.String("/"),
		MaxKeys:           aws.Int64(int64(n)),
	})
	if err != nil {
		return nil, err
	}

	f.readDirContToken = resp.NextContinuationToken
	if !(*resp.IsTruncated) {
		f.readDirNotTruncated = true
	}
	var infos = make([]fs.DirEntry, 0, len(resp.CommonPrefixes)+len(resp.Contents))

	for _, subdir := range resp.CommonPrefixes {
		infos = append(infos, newDirInfo(*subdir.Prefix))
	}
	for _, file := range resp.Contents {
		if strings.HasSuffix(*file.Key, "/") {
			continue
		}
		infos = append(infos, newFileInfo(*file.Key, *file.Size, *file.LastModified))
	}

	return infos, nil
}

func (f *s3File) ReadDirAll() ([]fs.DirEntry, error) {
	var entries []fs.DirEntry
	for {
		infos, err := f.ReadDir(1000)
		entries = append(entries, infos...)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
	}
	return entries, nil
}

func (f *s3File) Stat() (fs.FileInfo, error) {
	var err error
	if f.info == nil {
		f.info, err = f.fs.Stat(f.Name())
		if err != nil {
			return nil, err
		}
	}
	return f.info, nil
}

func (f *s3File) Close() error {
	if f.in == nil {
		return nil
	}
	err := f.in.Close()
	f.in = nil
	return err
}

func (f *s3File) ReadAt(p []byte, off int64) (int, error) {
	if _, err := f.Seek(off, io.SeekStart); err != nil {
		return 0, err
	}
	return f.Read(p)
}

func (f *s3File) Read(p []byte) (int, error) {
	var err error
	if f.in == nil {
		f.in, err = f.rangeReader(f.offset, int64(len(p)))
		if err != nil {
			return 0, err
		}
	}
	n, err := f.in.Read(p)
	if err == io.EOF {
		if f.in != nil {
			f.in.Close()
			f.in = nil
		}
		err = nil
	}
	f.offset += int64(n)
	if f.offset >= f.info.Size() {
		return int(n), io.EOF
	}
	return int(n), err
}

func (f *s3File) Seek(offset int64, whence int) (int64, error) {
	if f.in == nil {
		return 0, fs.ErrClosed
	}
	start := f.offset
	switch whence {
	case io.SeekStart:
		start = offset
	case io.SeekCurrent:
		start = f.offset + offset
	case io.SeekEnd:
		start = f.info.Size() - offset
	}
	if start < 0 {
		return start, fs.ErrInvalid
	}
	if f.offset < start {
		if f.in != nil {
			f.in.Close()
			f.in = nil
		}
	}
	f.offset = start
	f.in = nil
	return start, nil
}
