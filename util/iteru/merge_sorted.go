package iteru

import (
	"iter"

	"reduction.dev/reduction/util/ds"
)

// MergeSorted combines multiple sorted iterators into a single sorted iterator.
func MergeSorted[T any](iters []iter.Seq[T], compare func(a, b T) int) iter.Seq[T] {
	// Track iterator state in heap
	type iterItem struct {
		next func() (T, bool)
		item T
	}

	// Create heap ordered by value
	heap := ds.NewHeap(func(a, b iterItem) int {
		return compare(a.item, b.item)
	}, len(iters))

	// Create a pull-style iterator for each input iterator
	stopFns := make([]func(), len(iters))

	// Initialize heap with first value from each iterator
	for i, it := range iters {
		next, stop := iter.Pull(it)
		stopFns[i] = stop

		// If the iterator has a matching value, add it to the heap.
		item, ok := next()
		if ok {
			heap.Push(iterItem{next, item})
		}
	}

	return func(yield func(T) bool) {
		defer func() {
			for _, fn := range stopFns {
				fn()
			}
		}()

		for {
			ii, ok := heap.Pop()
			if !ok {
				return
			}

			// Yield the value
			if !yield(ii.item) {
				return
			}

			// Add another item to the heap from the used iterator if there is one
			item, ok := ii.next()
			if ok {
				heap.Push(iterItem{ii.next, item})
			}
		}
	}
}
