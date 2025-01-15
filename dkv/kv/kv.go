package kv

import (
	"bytes"
	"errors"
	"iter"

	"reduction.dev/reduction/dkv/mergesort"
)

type Entry interface {
	Key() []byte
	Value() []byte
	IsDelete() bool
	SeqNum() uint64
}

func MergeEntries(iters []iter.Seq[Entry]) iter.Seq[Entry] {
	return mergesort.Merge(iters, AscendingEntries, keepNewest)
}

func AscendingEntries(a, b Entry) int {
	return bytes.Compare(a.Key(), b.Key())
}

func keepNewest(a, b Entry) Entry {
	if a.SeqNum() > b.SeqNum() {
		return a
	}
	return b
}

var ErrNotFound = errors.New("NotFound")
