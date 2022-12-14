package s3fs

import (
	"io/fs"
	"path"
	"time"
)

// s3FileInfo implements both, fs.FileInfo and fs.DirEntry
type s3FileInfo struct {
	name    string
	size    int64
	modTime time.Time
}

func newFileInfo(name string, size int64, modTime time.Time) *s3FileInfo {
	return &s3FileInfo{
		name:    path.Base("/" + name),
		size:    size,
		modTime: modTime,
	}
}

// Name provides the base name of the file.
func (fi *s3FileInfo) Name() string {
	return fi.name
}

// Size provides the length in bytes for a file.
func (fi *s3FileInfo) Size() int64 {
	return fi.size
}

// Mode provides the file mode bits. For a file in S3 this defaults to
// 664 for files, 775 for directories.
// In the future this may return differently depending on the permissions
// available on the bucket.
func (fi *s3FileInfo) Mode() fs.FileMode {
	return 0664
}

func (fi *s3FileInfo) Type() fs.FileMode {
	return fi.Mode()
}

// ModTime provides the last modification time.
func (fi *s3FileInfo) ModTime() time.Time {
	return fi.modTime
}

// IsDir provides the abbreviation for Mode().IsDir()
func (fi *s3FileInfo) IsDir() bool {
	return false
}

// Sys provides the underlying data source (can return nil)
func (fi *s3FileInfo) Sys() any {
	return nil
}

func (fi *s3FileInfo) Info() (fs.FileInfo, error) {
	return fi, nil
}
