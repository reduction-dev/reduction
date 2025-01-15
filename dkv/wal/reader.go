package wal

import (
	"errors"
	"fmt"
	"io"
	"iter"

	"reduction.dev/reduction/dkv/fields"
	"reduction.dev/reduction/dkv/kv"
	"reduction.dev/reduction/dkv/storage"
)

type Reader struct {
	file       storage.File
	startAfter uint64
}

var ErrNotFound = errors.New("wal file not found")

func NewReader(fs storage.FileSystem, handle Handle) *Reader {
	f := fs.Open(handle.URI())
	return &Reader{file: f, startAfter: handle.After}
}

func (r *Reader) All() iter.Seq2[Entry, error] {
	return func(yield func(Entry, error) bool) {
		cursor := &storage.Cursor{File: r.file}

		// Read the first firstSeqNum in the file
		firstSeqNum, err := fields.ReadUint64(cursor)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				yield(Entry{}, fmt.Errorf("reading %s: %w", r.file.URI(), err))
				return
			}
			if errors.Is(err, io.EOF) { // File is empty, which is allowed
				return
			}
			yield(Entry{}, err)
			return
		}

		if (r.startAfter + 1) < firstSeqNum {
			panic(fmt.Sprintf("WAL reader is supposed to start after seqNum %d but firstSeqNum in WAL file is %d", r.startAfter, firstSeqNum))
		}

		skipEntriesCount := r.startAfter - firstSeqNum + 1
		cursor.Move(0)

		// Skip entries until we reach the next seqNum
		for i := skipEntriesCount; i > 0; i-- {
			if err := fields.SkipUint64(cursor); err != nil {
				yield(Entry{}, err)
				return
			}
			if err := fields.SkipVarBytes(cursor); err != nil {
				yield(Entry{}, err)
				return
			}
			wasDeleted, err := fields.ReadTombstone(cursor)
			if err != nil {
				yield(Entry{}, err)
				return
			}
			if wasDeleted {
				continue
			}
			if err := fields.SkipVarBytes(cursor); err != nil {
				yield(Entry{}, err)
				return
			}
		}

		// Read remaining entries
		for {
			seqNum, err := fields.ReadUint64(cursor)
			if errors.Is(err, io.EOF) {
				return // Stop iterating when reaching EOF
			} else if err != nil {
				yield(Entry{}, err)
				return
			}

			key, err := fields.ReadVarBytes(cursor)
			if err != nil {
				yield(Entry{}, err)
				return
			}

			wasDeleted, err := fields.ReadTombstone(cursor)
			if err != nil {
				yield(Entry{}, err)
				return
			}
			if wasDeleted {
				if !yield(Entry{K: key, Deleted: true}, nil) {
					return
				}
				continue
			}

			value, err := fields.ReadVarBytes(cursor)
			if err != nil {
				yield(Entry{}, err)
				return
			}
			if !yield(Entry{K: key, V: value, seqNum: seqNum}, err) {
				return
			}
		}
	}
}

type Entry struct {
	K []byte
	V []byte
	// seqNum is the Log Sequence Number. It's the latest record number that has
	// already been written to the log and monotonically increases.
	seqNum  uint64
	Deleted bool
}

func (w Entry) IsDelete() bool {
	return w.Deleted
}

func (w Entry) Key() []byte {
	return w.K
}

func (w Entry) Value() []byte {
	return w.V
}

func (w Entry) SeqNum() uint64 {
	return w.seqNum
}

var _ kv.Entry = Entry{}
