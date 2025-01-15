// Package heap provides an implementation of a binary heap.
// A binary heap (binary min-heap) is a tree with the property that each node
// is the minimum-valued node in its subtree.
//
// Started from https://github.com/zyedidia/generic/blob/master/heap/heap.go at 98022f9
package ds

// Heap implements a binary Heap.
type Heap[T any] struct {
	data        []T
	compare     func(a, b T) int
	assignIndex func(T, int)
}

// CompareFn is a function that returns:
//   - negative value if a < b
//   - zero if a == b
//   - positive value if a > b
type CompareFn[T any] func(a, b T) int

// NewHeap returns a new heap with the given compare function.
func NewHeap[T any](compare CompareFn[T], cap int) *Heap[T] {
	return &Heap[T]{
		data:    make([]T, 0, cap),
		compare: compare,
	}
}

// Add a function to call on the elements when their index in the heap changes.
func (h *Heap[T]) SetIndexAssigner(indexAssigner func(T, int)) {
	h.assignIndex = indexAssigner
}

// Push pushes the given element onto the heap.
func (h *Heap[T]) Push(x T) {
	h.data = append(h.data, x)
	if h.assignIndex != nil {
		h.assignIndex(x, len(h.data)-1)
	}
	h.up(len(h.data) - 1)
}

// Pop removes and returns the minimum element from the heap.
func (h *Heap[T]) Pop() (T, bool) {
	var x T
	if h.IsEmpty() {
		return x, false
	}

	x = h.data[0]
	n := len(h.data) - 1
	h.data[0] = h.data[n]
	if h.assignIndex != nil {
		h.assignIndex(x, -1)
		h.assignIndex(h.data[0], 0)
	}
	h.data = h.data[:n]
	if n > 0 {
		h.down(0)
	}
	return x, true
}

// Peek returns the minimum element from the heap without removing it. if the
// heap is empty, it returns zero value and false.
func (h *Heap[T]) Peek() (T, bool) {
	if h.Size() == 0 {
		var x T
		return x, false
	}

	return h.data[0], true
}

// Fix fixes the position of the element in the heap
func (h *Heap[T]) Fix(i int) {
	if i == -1 {
		return
	}

	if !h.down(i) {
		h.up(i)
	}
}

// Size returns the number of elements in the heap.
func (h *Heap[T]) Size() int {
	return len(h.data)
}

func (h *Heap[T]) IsEmpty() bool {
	return h.Size() == 0
}

func (h *Heap[T]) down(i int) bool {
	i0 := i
	for {
		left, right := 2*i+1, 2*i+2
		if left >= len(h.data) || left < 0 { // `left < 0` in case of overflow
			break
		}

		// find the smallest child
		j := left
		if right < len(h.data) && h.compare(h.data[right], h.data[left]) < 0 {
			j = right
		}

		if h.compare(h.data[j], h.data[i]) >= 0 {
			break
		}

		h.swap(i, j)
		i = j
	}
	return i > i0 // did move
}

func (h *Heap[T]) up(i int) {
	for {
		parent := (i - 1) / 2
		if i == 0 || h.compare(h.data[i], h.data[parent]) >= 0 {
			break
		}

		h.swap(i, parent)
		i = parent
	}
}

func (h *Heap[T]) swap(i, j int) {
	h.data[i], h.data[j] = h.data[j], h.data[i]
	if h.assignIndex != nil {
		h.assignIndex(h.data[i], i)
		h.assignIndex(h.data[j], j)
	}
}
