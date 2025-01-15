package ds

// QueuePartition is a partition managed by PartitionedPriorityQueue
type QueuePartition[T any] interface {
	Peek() (T, bool)
	Pop() (T, bool)
	Push(T)
	IsEmpty() bool
	Delete(T)
	AssignIndex(int)
	Index() int
}

// PartitionedPriorityQueue maintains a heap of queues where each queue represents
// a partition.
type PartitionedPriorityQueue[T any] struct {
	heap              *Heap[QueuePartition[T]]
	comparator        func(a, b T) int
	partitions        []QueuePartition[T]
	getPartitionIndex func(T) int
}

// NewPartitionedPriorityQueue creates a new queue with the given partitions.
// The comparator is used to order the partitions in the heap by compared the
// values returned by a partition.Peek(). The partitionIndex must return an
// index that addresses one of the partitions passed to the constructor.
func NewPartitionedPriorityQueue[T any](
	partitions []QueuePartition[T],
	comparator func(a, b T) int,
	partitionIndex func(T) int,
) *PartitionedPriorityQueue[T] {
	// Compare partitions by their minimum value returned from Peek. Empty
	// partitions sort to the end.
	heapCompare := func(a, b QueuePartition[T]) int {
		aVal, aOk := a.Peek()
		bVal, bOk := b.Peek()

		switch {
		case !aOk && !bOk:
			return 0
		case !aOk:
			return 1
		case !bOk:
			return -1
		default:
			return comparator(aVal, bVal)
		}
	}

	// Store the partitions in a heap and keep track of the IndexingHeap elements
	// in a slice.
	heap := NewHeap(heapCompare, len(partitions))
	heap.SetIndexAssigner(func(p QueuePartition[T], i int) {
		p.AssignIndex(i)
	})
	for _, partition := range partitions {
		heap.Push(partition)
	}

	return &PartitionedPriorityQueue[T]{
		heap:              heap,
		comparator:        comparator,
		partitions:        partitions,
		getPartitionIndex: partitionIndex,
	}
}

// Peek returns the smallest element across all partitions.
func (p *PartitionedPriorityQueue[T]) Peek() (T, bool) {
	queue, ok := p.heap.Peek()
	if !ok {
		var zero T
		return zero, false
	}

	return queue.Peek()
}

// Pop removes and returns the smallest element across all partitions.
func (p *PartitionedPriorityQueue[T]) Pop() (T, bool) {
	partition, ok := p.heap.Peek()
	if !ok {
		var zero T
		return zero, false
	}

	val, ok := partition.Pop()
	if !ok {
		var zero T
		return zero, false
	}

	// Fix heap order since root queue's minimum changed
	p.heap.Fix(partition.Index())
	return val, true
}

// IsEmpty returns true if all partitions are empty.
func (p *PartitionedPriorityQueue[T]) IsEmpty() bool {
	partition, ok := p.heap.Peek()
	if !ok {
		return true
	}

	_, ok = partition.Peek()
	return !ok
}

// Push adds an item to its corresponding partition
func (p *PartitionedPriorityQueue[T]) Push(item T) {
	partition := p.partitions[p.getPartitionIndex(item)]
	partition.Push(item)
	p.heap.Fix(partition.Index())
}

// Delete removes an item from its corresponding partition
func (p *PartitionedPriorityQueue[T]) Delete(item T) {
	partition := p.partitions[p.getPartitionIndex(item)]
	partition.Delete(item)
	p.heap.Fix(partition.Index())
}
