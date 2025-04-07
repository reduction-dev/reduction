package storage

import (
	"errors"
	"io"
	"strings"
)

type FileSystem interface {
	New(path string) File
	Open(path string) File
	Copy(source string, destination string) error
}

type File interface {
	io.ReaderAt
	io.Writer
	Save() error
	Name() string
	Delete() error
	URI() string
	Size() int64
	CreateDeleteFunc() func() error
}

type FileMode int

const FILE_MODE_READ = 0
const FILE_MODE_WRITE = 1

var ErrNotFound = errors.New("file not found")

func NewFileSystemFromLocation(location string) (FileSystem, error) {
	if strings.HasPrefix(location, s3Protocol) {
		return NewS3FileSystemFromURI(location)
	}

	if strings.HasPrefix(location, memoryProtocol) {
		workingDir := strings.TrimPrefix(location, memoryProtocol)
		return NewMemoryFilesystem().WithWorkingDir(workingDir), nil
	}

	return NewLocalFilesystem(location), nil
}
