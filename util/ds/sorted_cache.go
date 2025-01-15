package ds

import (
	"bytes"

	"github.com/google/btree"
)

type SortedCache struct {
	tree         *btree.BTreeG[[]byte]
	byteSize     uint64
	maxSizeBytes uint64
}

func NewSortedCache(maxSizeBytes uint64) *SortedCache {
	return &SortedCache{
		tree: btree.NewG(2, func(a, b []byte) bool {
			return bytes.Compare(a, b) == -1 // before
		}),
		maxSizeBytes: maxSizeBytes,
	}
}

func (s *SortedCache) Push(addValue []byte) {
	s.byteSize += uint64(len(addValue))
	s.tree.ReplaceOrInsert(addValue)
}

func (s *SortedCache) Pop() (min []byte, ok bool) {
	min, ok = s.tree.DeleteMin()
	if ok {
		s.byteSize -= uint64(len(min))
	}
	return min, ok
}

func (s *SortedCache) PopLast() (max []byte, ok bool) {
	max, ok = s.tree.DeleteMax()
	if ok {
		s.byteSize -= uint64(len(max))
	}
	return max, ok
}

func (s *SortedCache) Peek() (min []byte, ok bool) {
	return s.tree.Min()
}

func (s *SortedCache) Delete(key []byte) {
	deleted, ok := s.tree.Delete(key)
	if ok {
		s.byteSize -= uint64(len(deleted))
	}
}

func (s *SortedCache) IsEmpty() bool {
	return s.tree.Len() == 0
}

func (s *SortedCache) IsFull() bool {
	return s.byteSize >= s.maxSizeBytes
}
