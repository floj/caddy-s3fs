package s3fs

import (
	"io/fs"
	"time"
)

// fileInfo implements os.fileInfo for a file in S3.
type fileInfo struct {
	mTime time.Time
	name  string
	size  int64
}

// newFileInfo creates file cachedInfo.
func newFileInfo(name string, size int64, mTime time.Time) fileInfo {
	return fileInfo{
		name:  name,
		size:  size,
		mTime: mTime,
	}
}

// Name provides the base name of the file.
func (fi fileInfo) Name() string {
	return fi.name
}

func (fi fileInfo) Info() (fs.FileInfo, error) {
	return fi, nil
}

func (fi fileInfo) Type() fs.FileMode {
	return fi.Mode()
}

// Size provides the length in bytes for a file.
func (fi fileInfo) Size() int64 {
	return fi.size
}

// Mode provides the file mode bits. For a file in S3 this defaults to
// 664 for files, 775 for directories.
// In the future this may return differently depending on the permissions
// available on the bucket.
func (fi fileInfo) Mode() fs.FileMode {
	return 0664
}

// ModTime provides the last modification time.
func (fi fileInfo) ModTime() time.Time {
	return fi.mTime
}

// IsDir provides the abbreviation for Mode().IsDir()
func (fi fileInfo) IsDir() bool {
	return false
}

// Sys provides the underlying data source (can return nil)
func (fi fileInfo) Sys() interface{} {
	return nil
}
