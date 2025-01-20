package batching

import (
	"context"
	"sync"
	"time"

	"reduction.dev/reduction/clocks"
)

type EventBatcherParams struct {
	MaxDelay time.Duration
	MaxSize  int
	Timer    clocks.Timer
}

type EventBatcher[T any] struct {
	onBatchReady func([]T)
	maxSize      int           // Max number of batched events before flushing
	maxDelay     time.Duration // Max time to wait before flushing
	timer        clocks.Timer
	mu           sync.Mutex // Guard batch
	batch        []T
	batchToken   *struct{} // Tracks the current batch
}

func NewEventBatcher[T any](ctx context.Context, params EventBatcherParams) *EventBatcher[T] {
	if params.Timer == nil {
		params.Timer = &clocks.SystemTimer{}
	}

	batcher := &EventBatcher[T]{
		onBatchReady: func([]T) {}, // no-op by default
		timer:        params.Timer,
		maxSize:      params.MaxSize,
		maxDelay:     params.MaxDelay,
	}

	go func() {
		<-ctx.Done()
		batcher.mu.Lock()
		defer batcher.mu.Unlock()
		batcher.batchToken = &struct{}{}
	}()

	return batcher
}

func (b *EventBatcher[T]) OnBatchReady(fn func([]T)) {
	b.onBatchReady = fn
}

func (b *EventBatcher[T]) Add(event T) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Set the timer when starting a new batch
	if len(b.batch) == 0 {
		currentBatchToken := b.batchToken // Track the batch when setting timer
		b.timer.Set(b.maxDelay, func() {
			b.mu.Lock()
			defer b.mu.Unlock()

			// Skip flushing if the batch was flushed before the timer acquired the lock
			if currentBatchToken != b.batchToken {
				return
			}

			b.flushLocked()
		})
	}

	b.batch = append(b.batch, event)
	if len(b.batch) >= b.maxSize {
		b.flushLocked()
	}
}

// Caller must hold the mutex lock
func (b *EventBatcher[T]) flushLocked() {
	// Yield the current batch and start a new one
	flushingBatch := b.batch
	b.batch = make([]T, 0, len(flushingBatch))
	b.batchToken = &struct{}{}
	b.onBatchReady(flushingBatch)
}
