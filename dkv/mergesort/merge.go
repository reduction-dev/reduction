package mergesort

import (
	"iter"

	"reduction.dev/reduction/util/ds"
)

// A merge sort that removes duplicates. Duplicates are determined by the cmp
// function, which returns zero when items are considered the same. Then the provided
// pick function will determine which of the equal items to keep.
func Merge[T comparable](iters []iter.Seq[T], cmp func(a T, b T) int, pick func(a T, b T) T) iter.Seq[T] {
	// A struct to track the iterators and their most recently yielded values in a
	// heap.
	type iterItem struct {
		index int
		item  T
	}

	// Create a min heap to keep track of iterated items
	heap := ds.NewHeap(func(a, b iterItem) int {
		return cmp(a.item, b.item)
	}, len(iters))

	// For each iterator pull the first result from the iterator and put it in the
	// heap along with an index to locate the iterator.
	nextFns := make([]func() (T, bool), len(iters))
	for i, it := range iters {
		// Will consume all items so won't call stop.
		next, _ := iter.Pull(it)
		nextFns[i] = next

		// If the iterator has a matching value, add it to the heap.
		item, ok := next()
		if ok {
			heap.Push(iterItem{i, item})
		}
	}

	return func(yield func(item T) bool) {
		var prevItem *iterItem

		// Pull items off the heap until there are no more left.
		for {
			ii, ok := heap.Pop()
			if !ok {
				// Yield the final item at the end
				if prevItem != nil {
					yield(prevItem.item)
				}
				return
			}

			// Add another item to the heap from the used iterator if there is one
			item, ok := nextFns[ii.index]()
			if ok {
				heap.Push(iterItem{ii.index, item})
			}

			// If it's the first value, set prevItem and skip ahead
			if prevItem == nil {
				prevItem = &ii
				continue
			}

			// If the item is the same according to the comparator, use the pick
			// function to choose the item to keep.
			if cmp(prevItem.item, ii.item) == 0 {
				pickedItem := pick(prevItem.item, ii.item)
				if pickedItem == prevItem.item {
					continue
				}
				if pickedItem == ii.item {
					prevItem = &ii
					continue
				}
				panic("pick must return one of the provided arguments")
			}

			// If item is changing, yield the previous item and replace it with the
			// current item.
			if !yield(prevItem.item) {
				return
			}
			prevItem = &ii
		}
	}
}
