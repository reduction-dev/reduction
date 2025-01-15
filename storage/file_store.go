package storage

import (
	"io"
	"iter"
)

type FileStore interface {
	// Write data to the given file path. The path is relative to whatever path
	// prefixes the file store was initialized with. However the returned URI
	// represents the full path to this file.
	Write(path string, data io.Reader) (uri string, err error)
	Read(path string) ([]byte, error)
	List() iter.Seq2[string, error]
	URI(path string) (string, error)
	Copy(sourceURI string, destination string) error
}
