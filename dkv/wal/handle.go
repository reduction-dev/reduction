package wal

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"reduction.dev/reduction/dkv/storage"
)

// A Handle references a specific wal number and file, allowing it to be deleted
// when it's no longer needed. Handle's are only created within the context of a
// checkpoint.
type Handle struct {
	ID   int
	file storage.File

	// The sequence number to begin reading WAL entries. This is a recovery
	// startup optimization.
	After uint64
}

func (h Handle) Delete() error {
	return h.file.Delete()
}

func (h Handle) Name() string {
	return h.file.Name()
}

func (h Handle) URI() string {
	return h.file.URI()
}

func (h Handle) Document() HandleDocument {
	return HandleDocument{
		URI:   h.URI(),
		After: h.After,
	}
}

func NewHandle(fs storage.FileSystem, doc HandleDocument) Handle {
	// Get ID from URI
	idString := strings.TrimSuffix(filepath.Base(doc.URI), ".wal")
	id, err := strconv.Atoi(idString)
	if err != nil {
		panic(fmt.Sprintf("failed to get WAL ID from URI %s: %v", doc.URI, err))
	}

	return Handle{
		ID:    id,
		file:  fs.Open(doc.URI),
		After: doc.After,
	}
}

type HandleDocument struct {
	URI   string `json:"uri"`   // File location
	After uint64 `json:"after"` // Sequence number to begin reading after
}
