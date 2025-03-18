package dkvtest

import (
	"crypto/md5"
	"encoding/binary"

	"reduction.dev/reduction/dkv/kv"
)

type HashBasedDataOwnership struct {
	total uint64
	index uint64
}

func NewHashBasedDataOwnership(numPartitions, thisPartitionIndex int) *HashBasedDataOwnership {
	if numPartitions < 1 {
		panic("must have at least 1 partition group")
	}
	return &HashBasedDataOwnership{
		total: uint64(numPartitions),
		index: uint64(thisPartitionIndex),
	}
}

func (p *HashBasedDataOwnership) OwnsKey(key []byte) bool {
	hash := md5.Sum(key)
	hashNum := binary.LittleEndian.Uint64(hash[:8])
	keyIndex := hashNum % uint64(p.total)
	return keyIndex == uint64(p.index)
}

func (p *HashBasedDataOwnership) ExclusivelyOwnsTable(uri string, startKey []byte, endKey []byte) (bool, error) {
	startHash := md5.Sum(startKey)
	endHash := md5.Sum(endKey)
	startHashNum := binary.LittleEndian.Uint64(startHash[:8])
	endHashNum := binary.LittleEndian.Uint64(endHash[:8])
	startIndex := startHashNum % uint64(p.total)
	endIndex := endHashNum % uint64(p.total)
	return startIndex == uint64(p.index) && endIndex == uint64(p.index), nil
}

var _ kv.DataOwnership = (*HashBasedDataOwnership)(nil)
