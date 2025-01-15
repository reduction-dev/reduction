package ds_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"reduction.dev/reduction/util/ds"
	"reduction.dev/reduction/util/queues"
)

func TestHeap_PushAndPop(t *testing.T) {
	h := ds.NewHeap(func(a, b int) int { return a - b }, 10)
	queues.Push(h, 3, 1, 2)
	assert.Equal(t, []int{1, 2, 3}, queues.Drain(h))
}

func TestHeap_Peek(t *testing.T) {
	h := ds.NewHeap(func(a, b int) int { return a - b }, 10)
	queues.Push(h, 3, 1, 2)

	val, ok := h.Peek()
	assert.True(t, ok)
	assert.Equal(t, 1, val)

	h.Pop()
	val, ok = h.Peek()
	assert.True(t, ok)
	assert.Equal(t, 2, val)
}

func TestHeap_Size(t *testing.T) {
	h := ds.NewHeap(func(a, b int) int { return a - b }, 10)
	assert.Equal(t, 0, h.Size())

	h.Push(3)
	assert.Equal(t, 1, h.Size())

	h.Push(1)
	assert.Equal(t, 2, h.Size())

	h.Pop()
	assert.Equal(t, 1, h.Size())

	h.Pop()
	assert.Equal(t, 0, h.Size())
}

func TestHeap_Duplicates(t *testing.T) {
	h := ds.NewHeap(func(a, b int) int { return a - b }, 10)
	queues.Push(h, 1, 1, 2, 2, 3)
	assert.Equal(t, []int{1, 1, 2, 2, 3}, queues.Drain(h))
}

type indexedValue struct {
	value int
	index int
}

func TestHeap_Fix(t *testing.T) {
	h := ds.NewHeap(func(a, b *indexedValue) int { return a.value - b.value }, 10)
	h.SetIndexAssigner(func(v *indexedValue, i int) {
		v.index = i
	})

	v1 := &indexedValue{value: 3}
	v2 := &indexedValue{value: 1}
	v3 := &indexedValue{value: 2}

	queues.Push(h, v1, v2, v3)

	// Change value and fix position
	v1.value = 0
	h.Fix(v1.index)

	assert.Equal(t, []*indexedValue{v1, v2, v3}, queues.Drain(h))
}

func TestHeap_FixNonExistentIndex(t *testing.T) {
	h := ds.NewHeap(func(a, b *indexedValue) int { return a.value - b.value }, 10)
	h.SetIndexAssigner(func(v *indexedValue, i int) {
		v.index = i
	})

	// Fix with -1 should be a no-op
	h.Fix(-1)

	v1 := &indexedValue{value: 1}
	h.Push(v1)

	// Verify heap still functions normally
	val, ok := h.Pop()
	assert.True(t, ok)
	assert.Equal(t, 1, val.value)
}

// Verify that items are assigned an index when first pushed, not just when they are moved.
func TestHeap_FixUnmovedItem(t *testing.T) {
	h := ds.NewHeap(func(a, b *indexedValue) int { return a.value - b.value }, 10)
	h.SetIndexAssigner(func(v *indexedValue, i int) {
		v.index = i
	})

	// Need at least 4 items to expose the possible bug. Mistakenly trying to fix
	// at index 0 would cause no moves because the root and its children already
	// satisfy the heap property.
	//
	//     1 <- index 0
	//    / \
	//   2   3
	//  /
	// 4 <- this is the item we will try to fix
	v1 := &indexedValue{value: 1}
	v2 := &indexedValue{value: 2}
	v3 := &indexedValue{value: 3}
	v4 := &indexedValue{value: 4}
	queues.Push(h, v1, v2, v3, v4)

	assert.Equal(t, v1.index, 0)
	assert.Equal(t, v2.index, 1)
	assert.Equal(t, v3.index, 2)
	assert.Equal(t, v4.index, 3)

	// Modify the last value so that it should be the minimum
	v4.value = 0
	h.Fix(v4.index) // This would fail if index weren't assigned on push

	peeked, ok := h.Peek()
	assert.True(t, ok)
	assert.Equal(t, 0, peeked.value, "v4 should be at root")

	assert.Equal(t, []*indexedValue{v4, v1, v2, v3}, queues.Drain(h))
}
