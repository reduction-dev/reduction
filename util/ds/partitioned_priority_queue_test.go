package ds_test

import (
	"cmp"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"reduction.dev/reduction/util/ds"
	"reduction.dev/reduction/util/queues"
)

func TestPartitionedPriorityQueue_NoPartitions(t *testing.T) {
	ppq := ds.NewPartitionedPriorityQueue(
		[]ds.QueuePartition[int]{},
		cmp.Compare[int],
		requirePartitionIndexNotCalled(t),
	)
	assert.True(t, ppq.IsEmpty())
	assert.Empty(t, queues.Drain(ppq))
}

func TestPartitionedPriorityQueue_SinglePartition(t *testing.T) {
	partition := &sortedIntPartition{}
	queues.Push(partition, 0, 1, 2)

	ppq := ds.NewPartitionedPriorityQueue(
		[]ds.QueuePartition[int]{partition},
		cmp.Compare[int],
		requirePartitionIndexNotCalled(t),
	)

	assert.False(t, ppq.IsEmpty())
	val, ok := ppq.Peek()
	assert.True(t, ok)
	assert.Equal(t, 0, val)

	assert.Equal(t, []int{0, 1, 2}, queues.Drain(ppq))
	assert.True(t, ppq.IsEmpty())
}

func TestPartitionedPriorityQueue_MultiplePartitions(t *testing.T) {
	ppq := ds.NewPartitionedPriorityQueue(
		[]ds.QueuePartition[int]{
			&sortedIntPartition{},
			&sortedIntPartition{},
			&sortedIntPartition{},
		},
		cmp.Compare[int],
		func(item int) int { return item % 3 },
	)

	queues.Push(ppq, 0, 1, 2, 3, 4, 5, 6, 7, 8)

	assert.Equal(t, []int{0, 1, 2, 3, 4, 5, 6, 7, 8}, queues.Drain(ppq))
	assert.True(t, ppq.IsEmpty())
}

func TestPartitionedPriorityQueue_MixedEmptyPartitions(t *testing.T) {
	ppq := ds.NewPartitionedPriorityQueue(
		[]ds.QueuePartition[int]{
			&sortedIntPartition{},
			&sortedIntPartition{},
			&sortedIntPartition{},
		},
		cmp.Compare[int],
		func(item int) int { return item % 3 },
	)

	queues.Push(ppq, 0, 1, 2)

	assert.Equal(t, []int{0, 1, 2}, queues.Drain(ppq))
	assert.True(t, ppq.IsEmpty())
}

func TestPartitionedPriorityQueue_Push(t *testing.T) {
	ppq := ds.NewPartitionedPriorityQueue(
		[]ds.QueuePartition[int]{
			&sortedIntPartition{},
			&sortedIntPartition{},
			&sortedIntPartition{},
		},
		cmp.Compare[int],
		func(item int) int { return item % 3 },
	)

	queues.Push(ppq, 0, 1, 2, 3, 4, 5, 6, 7, 8)

	// Verify the order of elements
	assert.Equal(t, []int{0, 1, 2, 3, 4, 5, 6, 7, 8}, queues.Drain(ppq))
	assert.True(t, ppq.IsEmpty())
}

func TestPartitionedPriorityQueue_PushSinglePartition(t *testing.T) {
	// Create a partitioned priority queue with a single partition
	ppq := ds.NewPartitionedPriorityQueue(
		[]ds.QueuePartition[int]{&sortedIntPartition{}},
		cmp.Compare[int],
		func(item int) int { return 0 }, // All items go to partition 0
	)

	queues.Push(ppq, 0, 1, 2)

	// Verify the order of elements
	assert.Equal(t, []int{0, 1, 2}, queues.Drain(ppq))
	assert.True(t, ppq.IsEmpty())
}

func TestPartitionedPriorityQueue_PushModifyingPartitionOrder(t *testing.T) {
	ppq := ds.NewPartitionedPriorityQueue(
		[]ds.QueuePartition[int]{
			&sortedIntPartition{},
			&sortedIntPartition{},
			&sortedIntPartition{},
		},
		cmp.Compare[int],
		func(item int) int { return item % 3 },
	)

	queues.Push(ppq, 3, 2, 1, 0)

	assert.Equal(t, []int{0, 1, 2, 3}, queues.Drain(ppq))
	assert.True(t, ppq.IsEmpty())
}

func TestPartitionedPriorityQueue_Delete(t *testing.T) {
	// Create a partitioned priority queue with multiple partitions
	ppq := ds.NewPartitionedPriorityQueue(
		[]ds.QueuePartition[int]{
			&sortedIntPartition{},
			&sortedIntPartition{},
			&sortedIntPartition{},
		},
		cmp.Compare[int],
		func(item int) int { return item % 3 },
	)

	queues.Push(ppq, 5, 4, 3, 2, 1, 0)

	// Delete some elements
	ppq.Delete(1) // From partition 1
	ppq.Delete(0) // From partition 0
	ppq.Delete(5) // From partition 2

	// Verify remaining elements are in order
	assert.Equal(t, []int{2, 3, 4}, queues.Drain(ppq))
	assert.True(t, ppq.IsEmpty())
}

func requirePartitionIndexNotCalled(t *testing.T) func(item int) int {
	t.Helper()
	return func(item int) int { require.FailNow(t, "partitionIndex called"); return 0 }
}

// sortedIntPartition implements QueuePartition by maintaining a sorted slice
type sortedIntPartition struct {
	items []int
	index int
}

func (p *sortedIntPartition) Peek() (int, bool) {
	if len(p.items) == 0 {
		return 0, false
	}
	return p.items[0], true
}

func (p *sortedIntPartition) Pop() (int, bool) {
	if len(p.items) == 0 {
		return 0, false
	}
	val := p.items[0]
	p.items = p.items[1:]
	return val, true
}

func (p *sortedIntPartition) Push(x int) {
	p.items = append(p.items, x)
	slices.Sort(p.items)
}

func (p *sortedIntPartition) AssignIndex(i int) {
	p.index = i
}

func (p *sortedIntPartition) Index() int {
	return p.index
}

func (p *sortedIntPartition) Delete(x int) {
	for i := range p.items {
		if p.items[i] == x {
			p.items = append(p.items[:i], p.items[i+1:]...)
			return
		}
	}
}

func (p *sortedIntPartition) IsEmpty() bool {
	return len(p.items) == 0
}

var _ ds.QueuePartition[int] = &sortedIntPartition{}
