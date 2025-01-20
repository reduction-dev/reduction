package batching_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"reduction.dev/reduction/clocks"
	"reduction.dev/reduction/rpc/batching"
)

func TestEventBatcherFlushesOnSize(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	timer := &clocks.FakeTimer{}
	receivedBatches := make(chan []int, 2)

	batcher := batching.NewEventBatcher[int](ctx, batching.EventBatcherParams{
		MaxDelay: time.Second, // unused
		MaxSize:  2,
		Timer:    timer,
	})
	batcher.OnBatchReady(func(events []int) {
		receivedBatches <- events
	})

	// Adding 2 events should trigger flush due to size, the 3rd event should be
	// in the next batch.
	batcher.Add(1)
	batcher.Add(2)
	batcher.Add(3)

	batch := <-receivedBatches
	require.Len(t, batch, 2)
	assert.Equal(t, []int{1, 2}, batch)
}

func TestEventBatcherFlushesOnTime(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	timer := &clocks.FakeTimer{}
	receivedBatches := make(chan []int, 2)

	batcher := batching.NewEventBatcher[int](ctx, batching.EventBatcherParams{
		MaxDelay: time.Second, // unused
		MaxSize:  10,
		Timer:    timer,
	})
	batcher.OnBatchReady(func(events []int) {
		receivedBatches <- events
	})

	// Add events but don't exceed size limit
	batcher.Add(1)
	batcher.Add(2)

	// Trigger timer callback
	timer.Trigger()

	batch := <-receivedBatches
	require.Len(t, batch, 2)
}

func TestEventBatcherResetsBatchAfterFlush(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	timer := &clocks.FakeTimer{}
	receivedBatches := make(chan []int, 2)

	batcher := batching.NewEventBatcher[int](ctx, batching.EventBatcherParams{
		MaxDelay: time.Second,
		MaxSize:  2,
		Timer:    timer,
	})
	batcher.OnBatchReady(func(events []int) {
		receivedBatches <- events
	})

	// First batch
	batcher.Add(1)
	batcher.Add(2)

	batch := <-receivedBatches
	require.Len(t, batch, 2)
	assert.Equal(t, []int{1, 2}, batch)

	// Second batch
	batcher.Add(3)
	batcher.Add(4)

	batch = <-receivedBatches
	require.Len(t, batch, 2)
	assert.Equal(t, []int{3, 4}, batch)
}
