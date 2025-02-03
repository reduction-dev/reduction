package batching

import (
	"context"
)

type BatchFetcher[T, R any] func(ctx context.Context, events []T) ([]R, error)

// ReorderFetcher uses an EventBatcher and ReorderBuffer to asynchronously fetch
// batches and ensuring they are returned in order on the output Channel.
type ReorderFetcher[T, R any] struct {
	Output chan R

	batcher    *EventBatcher[T]
	fetchBatch BatchFetcher[T, R]
	errChan    chan error
	buffer     *ReorderBuffer[[]R]
}

type NewReorderFetcherParams[T, R any] struct {
	Batcher    *EventBatcher[T]
	FetchBatch BatchFetcher[T, R]
	ErrChan    chan error
	BufferSize int
}

func NewReorderFetcher[T, R any](ctx context.Context, params NewReorderFetcherParams[T, R]) *ReorderFetcher[T, R] {
	reorderBufferSize := params.BufferSize
	if reorderBufferSize == 0 {
		// Must allow at least 1 item into the buffer to make progress.
		reorderBufferSize = 1
	}

	rf := &ReorderFetcher[T, R]{
		batcher:    params.Batcher,
		fetchBatch: params.FetchBatch,
		Output:     make(chan R, params.BufferSize),
		errChan:    params.ErrChan,
		buffer:     NewReorderBuffer[[]R](reorderBufferSize),
	}

	// Process batch timeouts
	go func() {
		for {
			select {
			case <-rf.batcher.BatchTimedOut:
				rf.flush(ctx, CurrentBatch)
			case <-ctx.Done():
				return
			}
		}
	}()

	return rf
}

// Add an event to the batch and flush if batch is full.
func (d *ReorderFetcher[T, R]) Add(ctx context.Context, event T) {
	d.batcher.Add(event)
	if d.batcher.IsFull() {
		d.flush(ctx, CurrentBatch)
	}
}

// Flush is the public method to call to flush the current batch.
func (d *ReorderFetcher[T, R]) Flush(ctx context.Context) {
	d.flush(ctx, CurrentBatch)
}

// flush the current batch and then asynchronously run the `FetchBatch` callback.
func (d *ReorderFetcher[T, R]) flush(ctx context.Context, token BatchToken) {
	events := d.batcher.Flush(token)
	if d.batcher == nil {
		panic("batcher became nil")
	}
	if len(events) == 0 {
		return
	}

	seqNum := d.buffer.Reserve()
	go func() {
		result, err := d.fetchBatch(ctx, events)
		if err != nil {
			d.errChan <- err
		}
		d.buffer.Add(seqNum, result)
		for resp := range d.buffer.Drain() {
			for _, result := range resp {
				d.Output <- result
			}
		}
	}()
}
