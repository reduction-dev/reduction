package batching_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"reduction.dev/reduction/batching"
	"reduction.dev/reduction/clocks"
)

func TestEventBatcher2_FlushesOnSize(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	timer := &clocks.FakeTimer{}
	batcher := batching.NewEventBatcher[int](ctx, batching.EventBatcherParams{
		MaxDelay: time.Second,
		MaxSize:  2,
		Timer:    timer,
	})

	batcher.Add(1)
	assert.False(t, batcher.IsFull())

	batcher.Add(2)
	assert.True(t, batcher.IsFull())

	batch := batcher.Flush(batching.CurrentBatch)
	assert.Equal(t, []int{1, 2}, batch)
}

func TestEventBatcher2_FlushesOnTimer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	timer := &clocks.FakeTimer{}
	batcher := batching.NewEventBatcher[int](ctx, batching.EventBatcherParams{
		MaxDelay: time.Second,
		MaxSize:  10,
		Timer:    timer,
	})

	batcher.Add(1)
	batcher.Add(2)

	go timer.Trigger()
	batchToken := <-batcher.BatchTimedOut
	batch := batcher.Flush(batchToken)
	assert.Equal(t, []int{1, 2}, batch)
}

func TestEventBatcher2_TokenInvalidation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	timer := &clocks.FakeTimer{}
	batcher := batching.NewEventBatcher[int](ctx, batching.EventBatcherParams{
		MaxDelay: time.Second,
		MaxSize:  10,
		Timer:    timer,
	})

	batcher.Add(1) // First add sets the timer

	// Trigger the timer and save the token
	go timer.Trigger()
	token := <-batcher.BatchTimedOut

	// Flush the batch for which the timer was set
	firstBatch := batcher.Flush(batching.CurrentBatch)
	assert.Equal(t, []int{1}, firstBatch)

	batcher.Add(2)
	batch := batcher.Flush(token)
	assert.Nil(t, batch, "Flush with old token should return nil")
}
