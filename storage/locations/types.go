package locations

import (
	"errors"
	"io"
	"iter"
)

// FileOp represents the type of file operation
type FileOp string

const (
	OpCreate FileOp = "create"
	OpRemove FileOp = "remove"
)

// FileEvent represents a file operation event with path and operation type
type FileEvent struct {
	Path string
	Op   FileOp
}

type StorageLocation interface {
	// Write data to the given file path. The path is relative to whatever path
	// prefixes the file store was initialized with. However the returned URI
	// represents the full path to this file.
	Write(path string, data io.Reader) (uri string, err error)
	Read(path string) ([]byte, error)
	List() iter.Seq2[string, error]
	URI(path string) (string, error)
	Copy(sourceURI string, destination string) error
	Remove(paths ...string) error
}

var ErrNotFound = errors.New("path not found")
