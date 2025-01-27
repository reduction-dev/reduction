package batching

import (
	"context"
	"sync"
	"time"

	"reduction.dev/reduction/clocks"
)

type BatchToken int64

var CurrentBatch BatchToken = BatchToken(-1)

type EventBatcherParams struct {
	MaxDelay time.Duration
	MaxSize  int
	Timer    clocks.Timer
}

type EventBatcher[T any] struct {
	maxSize       int           // Max number of batched events before flushing
	maxDelay      time.Duration // Max time to wait before flushing
	timer         clocks.Timer
	mu            sync.Mutex // Guard batch
	batch         []T
	batchToken    BatchToken
	BatchTimedOut chan BatchToken
	ctx           context.Context
	closed        bool
}

func NewEventBatcher[T any](ctx context.Context, params EventBatcherParams) *EventBatcher[T] {
	if params.Timer == nil {
		params.Timer = &clocks.SystemTimer{}
	}

	if params.MaxSize == 0 {
		params.MaxSize = 1
	}

	batcher := &EventBatcher[T]{
		timer:         params.Timer,
		maxSize:       params.MaxSize,
		maxDelay:      params.MaxDelay,
		batchToken:    0,
		batch:         make([]T, 0, params.MaxSize),
		BatchTimedOut: make(chan BatchToken),
		ctx:           ctx,
	}

	// Close the BatchTimedOut channel when the context is done
	go func() {
		<-ctx.Done()
		batcher.close()
	}()

	return batcher
}

func (b *EventBatcher[T]) Add(event T) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Set the timer when starting a new batch
	if len(b.batch) == 0 && b.maxDelay > 0 {
		currentBatchToken := b.batchToken // Track the batch when setting timer
		b.timer.Set(b.maxDelay, func() {
			select {
			case <-b.ctx.Done():
				return
			default:
				b.BatchTimedOut <- currentBatchToken
			}
		})
	}

	b.batch = append(b.batch, event)
}

func (b *EventBatcher[T]) IsFull() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	return len(b.batch) >= b.maxSize
}

func (b *EventBatcher[T]) Flush(token BatchToken) []T {
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
	b.batchToken = b.batchToken + 1
	b.timer.Stop()
	return flushingBatch
}

func (b *EventBatcher[T]) close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.closed {
		close(b.BatchTimedOut)
		b.closed = true
	}
}
