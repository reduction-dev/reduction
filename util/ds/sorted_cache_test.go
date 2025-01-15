package ds_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"reduction.dev/reduction/util/ds"
	"reduction.dev/reduction/util/queues"
	"reduction.dev/reduction/util/size"
)

func TestSortedCache_AddAndPop(t *testing.T) {
	ss := ds.NewSortedCache(size.KB)

	// Push items out of order
	queues.Push(ss, []byte{3}, []byte{1}, []byte{2})

	// Add a duplicate item
	ss.Push([]byte{2})

	// Pop the 3 items from the set to show ascending order
	assert.Equal(t, [][]byte{{1}, {2}, {3}}, queues.Drain(ss), "popped items should be in ascending order")

	// Push three more items
	queues.Push(ss, []byte{3}, []byte{1}, []byte{2})

	// PopLast the 3 items from the set to show descending order
	assert.Equal(t, [][]byte{{3}, {2}, {1}}, queues.DrainBackwards(ss), "popLast items should be in descending order")
}

func TestSortedCache_FullAndEmpty(t *testing.T) {
	ss := ds.NewSortedCache(5) // Cache that can hold 5 bytes

	assert.True(t, ss.IsEmpty(), "new cache should be empty")
	assert.False(t, ss.IsFull(), "new cache should not be full")

	// Add 3 bytes
	ss.Push([]byte{1, 2, 3})
	assert.False(t, ss.IsEmpty(), "cache with data should not be empty")
	assert.False(t, ss.IsFull(), "cache with 3/5 bytes should not be full")

	// Add 2 more bytes to reach capacity
	ss.Push([]byte{4, 5})
	assert.True(t, ss.IsFull(), "cache at capacity should be full")

	// Remove data
	ss.Pop()
	assert.False(t, ss.IsFull(), "cache below capacity should not be full")

	// Remove remaining data
	ss.Pop()
	assert.True(t, ss.IsEmpty(), "cache with no data should be empty")
	assert.False(t, ss.IsFull(), "empty cache should not be full")
}
