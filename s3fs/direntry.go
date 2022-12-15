package s3fs

import (
	"io/fs"
	"time"
)

// FileInfo implements os.FileInfo for a file in S3.
type dirEntry struct {
	name string
}

// newFileInfo creates file cachedInfo.
func newDirEntry(name string) *dirEntry {
	return &dirEntry{
		name: name,
	}
}

// Name provides the base name of the file.
func (fi dirEntry) Name() string {
	return fi.name
}

func (fi dirEntry) Info() (fs.FileInfo, error) {
	return fi, nil
}

func (fi dirEntry) Type() fs.FileMode {
	return fi.Mode()
}

// Size provides the length in bytes for a file.
func (fi dirEntry) Size() int64 {
	return 0
}

// Mode provides the file mode bits. For a file in S3 this defaults to
// 664 for files, 775 for directories.
// In the future this may return differently depending on the permissions
// available on the bucket.
func (fi dirEntry) Mode() fs.FileMode {
	return 0755
}

// ModTime provides the last modification time.
func (fi dirEntry) ModTime() time.Time {
	return time.Unix(0, 0)
}

// IsDir provides the abbreviation for Mode().IsDir()
func (fi dirEntry) IsDir() bool {
	return true
}

// Sys provides the underlying data source (can return nil)
func (fi dirEntry) Sys() interface{} {
	return nil
}
