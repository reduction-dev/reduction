package batching

import (
	"iter"
	"sync"
)

// ReorderBuffer stores out-of-order items provided by Add() and returns then in
// order from the Drain() method.
type ReorderBuffer[T any] struct {
	items         map[uint64]T
	nextSeqNum    uint64
	drainedSeqNum uint64

	// Keep track of the number of reserved buffer slots in a channel
	// so Add() can block when the buffer is full.
	reserved chan (struct{})

	mu sync.Mutex
}

func NewReorderBuffer[T any](maxItems int) *ReorderBuffer[T] {
	return &ReorderBuffer[T]{
		items:    make(map[uint64]T),
		reserved: make(chan struct{}, maxItems),
	}
}

// Add an item with a sequence number to the buffer
func (b *ReorderBuffer[T]) Add(seq uint64, item T) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.items[seq] = item
}

// Reserve a spot in the buffer and return the sequence number. This method
// blocks until the buffer is no longer full.
func (b *ReorderBuffer[T]) Reserve() uint64 {
	b.reserved <- struct{}{}
	seq := b.nextSeqNum
	b.nextSeqNum++
	return seq
}

// Drain iterates over the ordered items of the buffer. Only one drain iterator
// can run at a time so this is safe to call concurrently.
func (b *ReorderBuffer[T]) Drain() iter.Seq[T] {
	return func(yield func(T) bool) {
		b.mu.Lock()
		defer b.mu.Unlock()

		for {
			if item, ok := b.items[b.drainedSeqNum]; ok {
				delete(b.items, b.drainedSeqNum)
				b.drainedSeqNum++

				<-b.reserved
				if !yield(item) {
					break
				}
			} else {
				break
			}
		}
	}
}
