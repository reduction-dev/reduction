package memtable

import (
	"errors"
	"iter"

	"reduction.dev/reduction/dkv/kv"
	"reduction.dev/reduction/dkv/sst"
	"reduction.dev/reduction/dkv/ziptree"
)

type MemTable struct {
	zt      *ziptree.ZipTree
	memSize uint64
	size    uint64 // The expected flush size of the memtable
}

var ErrFull = errors.New("memtable full")

func NewMemTable(targetSize uint64) *MemTable {
	return &MemTable{
		zt:      ziptree.New(),
		memSize: targetSize,
	}
}

// Gets the value for a key. Returns k.ErrNotFound if
// there is no matching key.
func (t *MemTable) Get(key []byte) (kv.Entry, error) {
	node, ok := t.zt.Get(key)
	if !ok {
		return nil, kv.ErrNotFound
	}
	return newEntryFromNode(node), nil
}

func (t *MemTable) Put(key []byte, value []byte, seqNum uint64) (full bool) {
	replaced := t.zt.Put(ziptree.NewNode(key, value, meta{seqNum, putFlag}))
	t.size += uint64(sst.KVFlushSize(key, value))
	if replaced != nil {
		t.size -= uint64(sst.KVFlushSize(replaced.Key, replaced.Value))
	}
	return t.size > uint64(t.memSize)
}

// Delete is really an insert, writing with a delete marker. Returns a `full`
// flag when the table is full but will still write the entry, so there's no
// need to retry.
func (t *MemTable) Delete(key []byte, seqNum uint64) (full bool) {
	replaced := t.zt.Put(ziptree.NewNode(key, nil, meta{seqNum, deleteFlag}))
	t.size += uint64(sst.KVFlushSize(key, nil))
	if replaced != nil {
		t.size -= uint64(sst.KVFlushSize(replaced.Key, replaced.Value))
	}
	return t.size > uint64(t.memSize)
}

// ScanPrefix returns all entries matching the prefix in ascending order. This
// method transparently omits deleted entries.
func (t *MemTable) ScanPrefix(prefix []byte) iter.Seq[kv.Entry] {
	return func(yield func(kv.Entry) bool) {
		for node := range t.zt.AscendPrefix(prefix) {
			if isDeleteOp(node) {
				continue
			}
			if !yield(newEntryFromNode(node)) {
				return
			}
		}
	}
}

// Returns all items in the table including deleted items. A current limitation
// of go generics is that they can't understand when type variables are
// satisfied by an interface. So we cast the type to the general kv.Entry type.
func (t *MemTable) All() iter.Seq[kv.Entry] {
	return func(yield func(kv.Entry) bool) {
		for node := range t.zt.AscendPrefix(nil) {
			if !yield(newEntryFromNode(node)) {
				return
			}
		}
	}
}

func (t *MemTable) Size() int {
	return int(t.size)
}
