package batching

import (
	"context"
	"sync"
	"time"

	"reduction.dev/reduction/clocks"
	"reduction.dev/reduction/proto/workerpb"
)

type EventBatcherParams struct {
	MaxDelay time.Duration
	MaxSize  int
	Timer    clocks.Timer
}

type EventBatcher struct {
	onBatchReady func([]*workerpb.Event)
	maxSize      int           // Max number of batched events before flushing
	maxDelay     time.Duration // Max time to wait before flushing
	timer        clocks.Timer
	mu           sync.Mutex // Guard batch
	batch        []*workerpb.Event
	batchNum     uint64 // Batch number tracked as cancellation token for timer
	batchesChan  chan []*workerpb.Event
}

func NewEventBatcher(ctx context.Context, params EventBatcherParams) *EventBatcher {
	if params.Timer == nil {
		params.Timer = &clocks.SystemTimer{}
	}

	batcher := &EventBatcher{
		onBatchReady: func([]*workerpb.Event) {}, // no-op by default
		timer:        params.Timer,
		maxSize:      params.MaxSize,
		maxDelay:     params.MaxDelay,
		batchesChan:  make(chan []*workerpb.Event, 1),
		batchNum:     1, // Reserve 0 for stopping the timer
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case batch, ok := <-batcher.batchesChan:
				if !ok {
					return
				}
				batcher.onBatchReady(batch)
			}
		}
	}()

	return batcher
}

func (b *EventBatcher) OnBatchReady(fn func([]*workerpb.Event)) {
	b.onBatchReady = fn
}

func (b *EventBatcher) Add(event *workerpb.Event) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Set the timer when starting a new batch
	if len(b.batch) == 0 {
		timerBatchNum := b.batchNum // Track the batchNum when setting timer
		b.timer.Set(b.maxDelay, func() {
			b.mu.Lock()
			defer b.mu.Unlock()

			// Skip flushing if the batch was flushed before the timer acquired the lock
			if timerBatchNum != b.batchNum {
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
func (b *EventBatcher) flushLocked() {
	// Yield the current batch and start a new one
	flushingBatch := b.batch
	b.batch = make([]*workerpb.Event, 0, len(flushingBatch))
	b.batchNum++
	b.batchesChan <- flushingBatch
}
