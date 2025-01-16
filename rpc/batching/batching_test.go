package batching_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"reduction.dev/reduction/clocks"
	"reduction.dev/reduction/proto/workerpb"
	"reduction.dev/reduction/rpc/batching"
)

func TestEventBatcherFlushesOnSize(t *testing.T) {
	timer := &clocks.FakeTimer{}
	receivedBatches := make(chan []*workerpb.Event, 2)

	batcher := batching.NewEventBatcher(batching.EventBatcherParams{
		MaxDelay: time.Second, // unused
		MaxSize:  2,
		Timer:    timer,
	})
	batcher.OnBatchReady(func(events []*workerpb.Event) {
		receivedBatches <- events
	})
	defer batcher.Close()

	event1 := &workerpb.Event{Event: &workerpb.Event_KeyedEvent{}}
	event2 := &workerpb.Event{Event: &workerpb.Event_KeyedEvent{}}
	event3 := &workerpb.Event{Event: &workerpb.Event_KeyedEvent{}}

	// Adding 2 events should trigger flush due to size, the 3rd event should be
	// in the next batch.
	batcher.Add(event1)
	batcher.Add(event2)
	batcher.Add(event3)

	batch := <-receivedBatches
	require.Len(t, batch, 2)
	assert.Equal(t, []*workerpb.Event{event1, event2}, batch)
}

func TestEventBatcherFlushesOnTime(t *testing.T) {
	timer := &clocks.FakeTimer{}
	receivedBatches := make(chan []*workerpb.Event, 2)

	batcher := batching.NewEventBatcher(batching.EventBatcherParams{
		MaxDelay: time.Second, // unused
		MaxSize:  10,
		Timer:    timer,
	})
	batcher.OnBatchReady(func(events []*workerpb.Event) {
		receivedBatches <- events
	})
	defer batcher.Close()

	event1 := &workerpb.Event{Event: &workerpb.Event_KeyedEvent{}}
	event2 := &workerpb.Event{Event: &workerpb.Event_KeyedEvent{}}

	// Add events but don't exceed size limit
	batcher.Add(event1)
	batcher.Add(event2)

	// Trigger timer callback
	timer.Trigger()

	batch := <-receivedBatches
	require.Len(t, batch, 2)
}

func TestEventBatcherResetsBatchAfterFlush(t *testing.T) {
	timer := &clocks.FakeTimer{}
	receivedBatches := make(chan []*workerpb.Event, 2)

	batcher := batching.NewEventBatcher(batching.EventBatcherParams{
		MaxDelay: time.Second,
		MaxSize:  2,
		Timer:    timer,
	})
	batcher.OnBatchReady(func(events []*workerpb.Event) {
		receivedBatches <- events
	})
	defer batcher.Close()

	event1 := &workerpb.Event{Event: &workerpb.Event_KeyedEvent{}}
	event2 := &workerpb.Event{Event: &workerpb.Event_KeyedEvent{}}
	event3 := &workerpb.Event{Event: &workerpb.Event_KeyedEvent{}}
	event4 := &workerpb.Event{Event: &workerpb.Event_KeyedEvent{}}

	// First batch
	batcher.Add(event1)
	batcher.Add(event2)

	batch := <-receivedBatches
	require.Len(t, batch, 2)
	assert.Equal(t, []*workerpb.Event{event1, event2}, batch)

	// Second batch
	batcher.Add(event3)
	batcher.Add(event4)

	batch = <-receivedBatches
	require.Len(t, batch, 2)
	assert.Equal(t, []*workerpb.Event{event3, event4}, batch)
}
