package sst

import "reduction.dev/reduction/dkv/kv"

// An entry read from a table that implements k.Table
type Entry struct {
	key      []byte
	value    []byte
	seqNum   uint64
	isDelete bool
}

func (e *Entry) IsDelete() bool {
	return e.isDelete
}

func (e *Entry) Key() []byte {
	return e.key
}

func (e *Entry) Value() []byte {
	return e.value
}

func (e *Entry) SeqNum() uint64 {
	return e.seqNum
}

var _ kv.Entry = (*Entry)(nil)

func FlushSize(e kv.Entry) uint32 {
	return uint32(EntryOverheadSize + len(e.Key()) + len(e.Value()))
}

func KVFlushSize(key []byte, value []byte) uint32 {
	return uint32(EntryOverheadSize + len(key) + len(value))
}

// 17 bytes of overhead (4 key length, 4 value length, 1 op, 8 seqNum)
const EntryOverheadSize = 17
