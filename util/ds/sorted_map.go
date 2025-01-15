package ds

import (
	"cmp"
	"iter"
	"slices"
)

type SortedMap[K cmp.Ordered, V any] struct {
	list     []K
	m        map[K]V
	isSorted bool
}

func NewSortedMap[K cmp.Ordered, V any]() *SortedMap[K, V] {
	return &SortedMap[K, V]{
		list: nil,
		m:    make(map[K]V),
	}
}

func (sm *SortedMap[K, V]) Set(k K, v V) (new bool) {
	_, hadKey := sm.m[k]
	sm.m[k] = v

	if !hadKey {
		sm.list = append(sm.list, k)
		sm.isSorted = false // invalidate sorting
	}

	return !hadKey
}

func (sm *SortedMap[K, V]) Get(k K) (V, bool) {
	v, ok := sm.m[k]
	return v, ok
}

func (sm *SortedMap[K, V]) Has(k K) bool {
	_, ok := sm.m[k]
	return ok
}

func (sm *SortedMap[K, V]) Values() []V {
	sm.ensureSorted()
	values := make([]V, len(sm.list))
	for i, k := range sm.list {
		values[i] = sm.m[k]
	}
	return values
}

func (sm *SortedMap[K, V]) Keys() []K {
	sm.ensureSorted()
	return sm.list
}

func (sm *SortedMap[K, V]) All() iter.Seq2[K, V] {
	sm.ensureSorted()
	return func(yield func(K, V) bool) {
		for _, k := range sm.list {
			if !yield(k, sm.m[k]) {
				return
			}
		}
	}
}

func (sm *SortedMap[K, V]) Delete(k K) (removed bool) {
	sm.ensureSorted()
	index, found := slices.BinarySearch(sm.list, k)
	if found {
		sm.list = slices.Delete(sm.list, index, index+1)
		delete(sm.m, k)
	}
	return found
}

func (sm *SortedMap[K, V]) Size() int {
	return len(sm.list)
}

func (sm *SortedMap[K, V]) ensureSorted() {
	if !sm.isSorted {
		slices.SortFunc(sm.list, cmp.Compare)
	}
}
