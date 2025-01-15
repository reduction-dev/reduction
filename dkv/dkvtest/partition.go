package dkvtest

import (
	"crypto/md5"
	"encoding/binary"
)

type Partition struct {
	total uint64
	index uint64
}

func NewPartition(numPartitions, thisPartitionIndex int) *Partition {
	if numPartitions < 1 {
		panic("must have at least 1 partition group")
	}
	return &Partition{
		total: uint64(numPartitions),
		index: uint64(thisPartitionIndex),
	}
}

func (p *Partition) OwnsKey(key []byte) bool {
	hash := md5.Sum(key)
	hashNum := binary.LittleEndian.Uint64(hash[:8])
	keyIndex := hashNum % uint64(p.total)
	return keyIndex == uint64(p.index)
}
