package s3fs

import (
	"io/fs"
	"path"
	"time"
)

type s3DirInfo struct {
	name string
}

func newDirInfo(name string) *s3DirInfo {
	return &s3DirInfo{
		name: path.Base("/" + name),
	}
}

// Name provides the base name of the file.
func (di *s3DirInfo) Name() string {
	return di.name
}

// Size provides the length in bytes for a file.
func (di *s3DirInfo) Size() int64 {
	return 0
}

func (di *s3DirInfo) Mode() fs.FileMode {
	return 0755
}

func (di *s3DirInfo) Type() fs.FileMode {
	return di.Mode()
}

func (di *s3DirInfo) ModTime() time.Time {
	return time.Unix(0, 0)
}

func (di *s3DirInfo) IsDir() bool {
	return true
}

func (di *s3DirInfo) Sys() any {
	return nil
}

func (di *s3DirInfo) Info() (fs.FileInfo, error) {
	return di, nil
}
