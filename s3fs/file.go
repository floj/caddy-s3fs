package s3fs

import (
	"errors"
	"io"
	"io/fs"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

// s3File represents a file in S3.
type s3File struct {
	fs.FileInfo // File info cached for later used

	fs   *S3FS  // Parent file system
	name string // Name of the file

	readdirContinuationToken *string // readdirContinuationToken is used to perform files listing across calls
	readdirNotTruncated      bool    // readdirNotTruncated is set when we shall continue reading

	off int64 // cur is the offset of the read-only stream

	streamRead io.ReadCloser // streamRead is the underlying stream we are reading from

	closed bool
}

const READAHEAD = 1024 * 64 // 64kb readahead

// newFile initializes an File object.
func newFile(fs *S3FS, name string) *s3File {
	return &s3File{
		fs:   fs,
		name: name,
	}
}

// Name returns the filename, i.e. S3 path without the bucket name.
func (f *s3File) Name() string { return f.name }

// Readdir reads the contents of the directory associated with file and
// returns a slice of up to n FileInfo values, as would be returned
// by ListObjects, in directory order. Subsequent calls on the same file will yield further FileInfos.
//
// If n > 0, Readdir returns at most n FileInfo structures. In this case, if
// Readdir returns an empty slice, it will return a non-nil error
// explaining why. At the end of a directory, the error is io.EOF.
//
// If n <= 0, Readdir returns all the FileInfo from the directory in
// a single slice. In this case, if Readdir succeeds (reads all
// the way to the end of the directory), it returns the slice and a
// nil error. If it encounters an error before the end of the
// directory, Readdir returns the FileInfo read until that point
// and a non-nil error.
func (f *s3File) ReadDir(n int) ([]fs.DirEntry, error) {
	if f.readdirNotTruncated {
		return nil, io.EOF
	}
	if n <= 0 {
		return f.readDirAll()
	}
	// ListObjects treats leading slashes as part of the directory name
	// It also needs a trailing slash to list contents of a directory.
	name := strings.TrimPrefix(f.Name(), "/") // + "/"

	// For the root of the bucket, we need to remove any prefix
	if name != "" && !strings.HasSuffix(name, "/") {
		name += "/"
	}
	rq := &s3.ListObjectsV2Input{
		ContinuationToken: f.readdirContinuationToken,
		Bucket:            aws.String(f.fs.bucket),
		Prefix:            aws.String(name),
		Delimiter:         aws.String("/"),
		MaxKeys:           aws.Int64(int64(n)),
	}
	output, err := f.fs.s3API.ListObjectsV2(rq)
	if err != nil {
		return nil, err
	}
	f.readdirContinuationToken = output.NextContinuationToken
	if !(*output.IsTruncated) {
		f.readdirNotTruncated = true
	}
	var fis = make([]fs.DirEntry, 0, len(output.CommonPrefixes)+len(output.Contents))
	for _, subfolder := range output.CommonPrefixes {
		fis = append(fis, NewFileInfo(path.Base("/"+*subfolder.Prefix), true, 0, time.Unix(0, 0)))
	}
	for _, fileObject := range output.Contents {
		if strings.HasSuffix(*fileObject.Key, "/") {
			continue
		}
		fis = append(fis, NewFileInfo(path.Base("/"+*fileObject.Key), false, *fileObject.Size, *fileObject.LastModified))
	}

	return fis, nil
}

// ReaddirAll provides list of file cachedInfo.
func (f *s3File) readDirAll() ([]fs.DirEntry, error) {
	var fileInfos []fs.DirEntry
	for {
		infos, err := f.ReadDir(1000)
		fileInfos = append(fileInfos, infos...)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			} else {
				return nil, err
			}
		}
	}
	return fileInfos, nil
}

// Stat returns the FileInfo structure describing file.
// If there is an error, it will be of type *PathError.
func (f *s3File) Stat() (fs.FileInfo, error) {
	info, err := f.fs.Stat(f.Name())
	if err == nil {
		f.FileInfo = info
	}
	return info, err
}

// Close closes the File, rendering it unusable for I/O.
// It returns an error, if any.
func (f *s3File) Close() error {
	f.closed = true
	// Closing a reading stream
	if f.streamRead != nil {
		// We try to close the Reader
		defer func() {
			f.streamRead = nil
		}()
		f.streamRead.Close()
	}

	// Or maybe we don't have anything to close
	return nil
}

// ReadAt reads len(p) bytes from the file starting at byte offset off.
// It returns the number of bytes read and the error, if any.
// ReadAt always returns a non-nil error when n < len(b).
// At end of file, that error is io.EOF.
func (f *s3File) ReadAt(p []byte, off int64) (n int, err error) {
	_, err = f.Seek(off, io.SeekStart)
	if err != nil {
		return
	}
	n, err = f.Read(p)
	return
}

// Read reads up to len(b) bytes from the File.
// It returns the number of bytes read and an error, if any.
// EOF is signaled by a zero count with err set to io.EOF.
func (f *s3File) Read(p []byte) (int, error) {
	var err error
	if f.streamRead == nil {
		f.streamRead, err = f.RangeReader(f.off, int64(len(p)))
		if err != nil {
			return 0, err
		}
	}
	n, err := f.streamRead.Read(p)
	if err == io.EOF {
		if f.streamRead != nil {
			f.streamRead.Close()
			f.streamRead = nil
		}
		err = nil
	}
	f.off += int64(n)
	if f.off >= f.Size() {
		return int(n), io.EOF
	}
	return int(n), err
}

// Seek sets the offset for the next Read or Write on file to offset, interpreted
// according to whence: 0 means relative to the origin of the file, 1 means
// relative to the current offset, and 2 means relative to the end.
// It returns the new offset and an error, if any.
// The behavior of Seek on a file opened with O_APPEND is not specified.
// seeking backwards invalidates the existing read buffer
func (f *s3File) Seek(offset int64, whence int) (int64, error) {
	if f.closed {
		return 0, fs.ErrClosed
	}
	startByte := f.off
	switch whence {
	case io.SeekStart:
		startByte = offset
	case io.SeekCurrent:
		startByte = f.off + offset
	case io.SeekEnd:
		startByte = f.Size() - offset
	}
	if startByte < 0 {
		return startByte, fs.ErrInvalid
	}
	if f.off < startByte {
		if f.streamRead != nil {
			f.streamRead.Close()
			f.streamRead = nil
		}
	}
	f.off = startByte
	f.streamRead = nil
	return startByte, nil
}
