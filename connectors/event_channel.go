package connectors

import (
	"context"
	"errors"
	"sync"
)

// ReadFunc is a function that reads events from a source and returns the
// results or an error.
type ReadFunc func() ([][]byte, error)

// NewEventChannel creates a channel of read functions that can be executed to
// read events from the source reader.
func NewEventChannel(ctx context.Context, sourceReader SourceReader) <-chan ReadFunc {
	channel := make(chan ReadFunc)
	processingDone := make(chan struct{})
	var closeOnce sync.Once

	sendReadFunc := func() {
		channel <- func() ([][]byte, error) {
			events, err := sourceReader.ReadEvents()
			if errors.Is(err, ErrEndOfInput) {
				// Close when we've reached the end of input
				closeOnce.Do(func() {
					close(channel)
				})
				return events, err
			}

			// Signal that processing is complete and the next function can be sent
			processingDone <- struct{}{}

			return events, err
		}
	}

	// Start a goroutine to manage the channel
	go func() {
		defer closeOnce.Do(func() { close(channel) })
		sendReadFunc()

		// Process next functions only after previous one completes
		for {
			select {
			case <-ctx.Done():
				return
			case <-processingDone:
				sendReadFunc()
			}
		}
	}()

	return channel
}
