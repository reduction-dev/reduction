package sst

import (
	"reduction.dev/reduction/dkv/bloom"
	"reduction.dev/reduction/dkv/fields"
	"reduction.dev/reduction/dkv/storage"
)

func (t *Table) writeFooter() int {
	t.entriesSize = t.Size()

	n := t.filter.Encode(t.file)
	n += t.searchIndex.Encode(t.file)

	// Write last 12 bytes
	n += fields.MustWriteUint64(t.file, uint64(t.entriesSize))
	n += fields.MustWriteUint32(t.file, 1) // Version 1
	return n
}

func (t *Table) loadFooter() {
	// Start reading at the last 12 bytes
	cur := &storage.Cursor{File: t.file}
	cur.Move(t.Size() - 12)

	// Move to the start of the meta footer data
	metaOffset, err := fields.ReadUint64(cur)
	if err != nil {
		panic(err)
	}
	cur.Move(int64(metaOffset))

	t.filter = bloom.Decode(cur)
	t.searchIndex = SearchIndexDecode(cur)
}
