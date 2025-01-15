package sst

import (
	"fmt"
	"iter"
	"math"
	"slices"
	"sync/atomic"

	"reduction.dev/reduction/dkv/fields"
	"reduction.dev/reduction/dkv/kv"
	"reduction.dev/reduction/dkv/storage"
)

// A conservative guess guess that an entry occupies 500 bytes. Used to get a
// starting capacity for buffered entries based on the size of the file.
const conservativeBytesPerEntry = 500

// Table writer is a factory for creating tables that keeps track of the table ID.
type TableWriter struct {
	fs storage.FileSystem
	id *atomic.Int64
}

// Create a TableWriter with a given file system and starting table ID.
func NewTableWriter(fs storage.FileSystem, id int64) *TableWriter {
	atomicNum := &atomic.Int64{}
	atomicNum.Store(id)
	return &TableWriter{fs: fs, id: atomicNum}
}

func (c *TableWriter) Write(entries iter.Seq[kv.Entry]) (*Table, error) {
	reservedNum := c.id.Add(1) - 1
	f := c.fs.New(fmt.Sprintf("%06d.sst", reservedNum))

	table := NewTable(f)
	for e := range entries {
		writeEntry(table, e)
	}

	table.size += int64(table.writeFooter())
	table.metadataLoaded = true
	return table, table.file.Save()
}

// Write one user-provided entry to the table.
//
// Panics if data cannot be written.
//
// Format:
//
//	<beginning_of_file>
//	[row 1]
//	[row 2]
//	...
//	[row N]
//	[meta block 1: bloom filter]
//	[meta block 2: search index]
//	[footer (12 bytes)]
//	<end_of_file>
func writeEntry(t *Table, entry kv.Entry) {
	// Set initial entry values
	if t.size == 0 {
		t.startKey = entry.Key()
		t.startSeqNum = entry.SeqNum()
	}
	// Set ending entry values
	t.endKey = entry.Key()
	t.endSeqNum = entry.SeqNum()

	// Add to metadata
	t.searchIndex.IndexOffset(t.size)
	t.filter.Add(entry.Key())

	t.size += int64(fields.MustWriteVarBytes(t.file, entry.Key()))
	t.size += int64(fields.MustWriteUint64(t.file, entry.SeqNum()))
	t.size += int64(fields.MustWriteTombstone(t.file, entry.IsDelete()))
	t.size += int64(fields.MustWriteVarBytes(t.file, entry.Value()))
}

func (c *TableWriter) WriteRun(entries iter.Seq[kv.Entry], targetSize uint64) ([]*Table, error) {
	var tables []*Table

	// The maximum size the buffer is allowed to reach before we flush
	maxBufferSize := int(math.Floor((float64(targetSize) * 1.5)))

	// A list of entries that are buffered, waiting to be written
	buffer := newEntryBuffer(maxBufferSize / conservativeBytesPerEntry)

	next, stop := iter.Pull(entries)
	defer stop()

	for {
		// Iterate through entries until we reach the ideal table size
		for buffer.size < int(targetSize) {
			entry, ok := next()
			if !ok {
				t, err := c.Write(buffer.all())
				if err != nil {
					return nil, err
				}
				return append(tables, t), nil
			}

			buffer.add(entry)
		}

		// Plan to cut at the set of entires that reaches targetSize
		buffer.cut()

		// Iterate until we reach the maximum size
		for buffer.size < maxBufferSize {
			entry, ok := next()
			if !ok {
				t, err := c.Write(buffer.all())
				if err != nil {
					return nil, err
				}
				return append(tables, t), nil
			}

			buffer.add(entry)
		}

		t, err := c.Write(buffer.flushChunk())
		if err != nil {
			return nil, err
		}
		tables = append(tables, t)
	}
}

// A stateful buffer of entries that tracks the size and chunk cut points.
type entryBuffer struct {
	entries   []kv.Entry
	size      int
	cutAt     int
	chunkSize int
}

func newEntryBuffer(cap int) *entryBuffer {
	return &entryBuffer{
		entries: make([]kv.Entry, 0, cap),
	}
}

func (b *entryBuffer) add(e kv.Entry) {
	b.entries = append(b.entries, e)
	b.size += int(FlushSize(e))
}

func (b *entryBuffer) cut() {
	b.cutAt = len(b.entries)
	b.chunkSize = b.size
}

func (b *entryBuffer) all() iter.Seq[kv.Entry] {
	return slices.Values(b.entries)
}

func (b *entryBuffer) flushChunk() iter.Seq[kv.Entry] {
	chunk := b.entries[:b.cutAt]
	b.entries = b.entries[b.cutAt:]
	b.size -= b.chunkSize
	return slices.Values(chunk)
}
