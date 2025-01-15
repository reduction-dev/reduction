package memtable

import (
	"reduction.dev/reduction/dkv/ziptree"
)

type flag = byte

const deleteFlag = flag(0b10000000)
const putFlag = flag(0b00000000)

// Value to store in ziptree node meta
type meta struct {
	seqNum uint64
	flag   flag
}

func isDeleteOp(node *ziptree.Node) bool {
	return node.Meta.(meta).flag == deleteFlag
}
