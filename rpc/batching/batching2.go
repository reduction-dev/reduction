package batching

import (
	"context"
	"sync"
	"time"

	"reduction.dev/reduction/clocks"
)

type BatchToken int64

var CurrentBatch BatchToken = BatchToken(-1)

type EventBatcherParams2 struct {
	MaxDelay time.Duration
	MaxSize  int
	Timer    clocks.Timer
}

type EventBatcher2[T any] struct {
	onBatchReady  func([]T)
	maxSize       int           // Max number of batched events before flushing
	maxDelay      time.Duration // Max time to wait before flushing
	timer         clocks.Timer
	mu            sync.Mutex // Guard batch
	batch         []T
	batchToken    BatchToken
	nextToken     BatchToken
	BatchTimedOut chan BatchToken
}

func NewEventBatcher2[T any](ctx context.Context, params EventBatcherParams2) *EventBatcher2[T] {
	if params.Timer == nil {
		params.Timer = &clocks.SystemTimer{}
	}

	if params.MaxSize == 0 {
		params.MaxSize = 1
	}

	batcher := &EventBatcher2[T]{
		onBatchReady:  func([]T) {}, // no-op by default
		timer:         params.Timer,
		maxSize:       params.MaxSize,
		maxDelay:      params.MaxDelay,
		batchToken:    0,
		nextToken:     1,
		BatchTimedOut: make(chan BatchToken),
	}

	go func() {
		<-ctx.Done()
		batcher.mu.Lock()
		defer batcher.mu.Unlock()
		batcher.batchToken = batcher.nextToken
		batcher.nextToken++
	}()

	return batcher
}

func (b *EventBatcher2[T]) Add(event T) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Set the timer when starting a new batch
	if len(b.batch) == 0 && b.maxDelay > 0 {
		currentBatchToken := b.batchToken // Track the batch when setting timer
		b.timer.Set(b.maxDelay, func() {
			b.BatchTimedOut <- currentBatchToken
		})
	}

	b.batch = append(b.batch, event)
}

func (b *EventBatcher2[T]) IsFull() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	return len(b.batch) >= b.maxSize
}

func (b *EventBatcher2[T]) Flush(token BatchToken) []T {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Early return if the batch is or the provided token doesn't match the
	// current batch.
	if len(b.batch) == 0 || (token != CurrentBatch && b.batchToken != token) {
		return nil
	}

	// return the current batch and start a new one
	flushingBatch := b.batch
	b.batch = make([]T, 0, len(flushingBatch))
	b.batchToken = b.nextToken
	b.nextToken++
	return flushingBatch
}
