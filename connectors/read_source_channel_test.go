package connectors_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"reduction.dev/reduction/connectors"
)

func TestNewReadSourceChannel_ReturnsEventsFromSourceReader(t *testing.T) {
	events := [][]byte{[]byte("event1"), []byte("event2")}
	reader := &fakeSourceReader{
		events:                  events,
		returnEOIAfterCallCount: 2,
	}

	channel := connectors.NewReadSourceChannel(t.Context(), reader)

	// Read first set of events
	readFunc := <-channel
	receivedEvents, err := readFunc()
	assert.NoError(t, err)
	assert.Equal(t, events, receivedEvents)
	assert.Equal(t, 1, reader.callCount)

	// Read second set (which will trigger EOI)
	readFunc = <-channel
	receivedEvents, err = readFunc()
	assert.NoError(t, err) // EOI is hidden from caller
	assert.Equal(t, events, receivedEvents)
	assert.Equal(t, 2, reader.callCount)

	// Channel should be closed after EOI
	_, ok := <-channel
	assert.False(t, ok, "channel should be closed after EOI")
}

func TestNewReadSourceChannel_PropagatesErrors(t *testing.T) {
	expectedErr := errors.New("read error")
	reader := &fakeSourceReader{
		err: expectedErr,
	}

	channel := connectors.NewReadSourceChannel(t.Context(), reader)
	readFunc := <-channel
	events, err := readFunc()
	assert.ErrorIs(t, err, expectedErr)
	assert.Nil(t, events)
}

func TestNewReadSourceChannel_ClosesOnContextCancellation(t *testing.T) {
	reader := &fakeSourceReader{
		events: [][]byte{[]byte("event")},
	}

	ctx, cancel := context.WithCancel(t.Context())
	channel := connectors.NewReadSourceChannel(ctx, reader)

	// Cancel the context
	cancel()

	// The channel should be closed soon
	assert.Eventually(t, func() bool {
		_, ok := <-channel
		return !ok
	}, time.Second, 10*time.Millisecond, "channel should be closed after context cancellation")
}

// fakeSourceReader is an implementation of the SourceReader interface
// that allows controlling what ReadEvents returns
type fakeSourceReader struct {
	events                  [][]byte
	err                     error
	callCount               int
	returnEOIAfterCallCount int // after this many calls, return ErrEndOfInput
	checkpoint              []byte
	connectors.UnimplementedSourceReader
}

func (r *fakeSourceReader) ReadEvents() ([][]byte, error) {
	r.callCount++
	if r.returnEOIAfterCallCount > 0 && r.callCount >= r.returnEOIAfterCallCount {
		return r.events, connectors.ErrEndOfInput // Still return events even with EOI error
	}
	return r.events, r.err
}

func (r *fakeSourceReader) Checkpoint() []byte {
	return r.checkpoint
}
