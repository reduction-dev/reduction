package memtable

import (
	"reduction.dev/reduction/dkv/kv"
	"reduction.dev/reduction/dkv/ziptree"
)

type Entry struct {
	key      []byte
	value    []byte
	isDelete bool
	seqNum   uint64
}

func newEntryFromNode(node *ziptree.Node) *Entry {
	return &Entry{
		key:      node.Key,
		value:    node.Value,
		seqNum:   node.Meta.(meta).seqNum,
		isDelete: isDeleteOp(node),
	}
}

func (e *Entry) IsDelete() bool {
	return e.isDelete
}

func (e *Entry) Key() []byte {
	return e.key
}

func (e *Entry) SeqNum() uint64 {
	return e.seqNum
}

func (e *Entry) Value() []byte {
	return e.value
}

var _ kv.Entry = (*Entry)(nil)
