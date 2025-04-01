package connectors

import (
	"context"
	"errors"
)

// ReadFunc is a function that reads events from a source and returns the
// results or an error.
type ReadFunc func() ([][]byte, error)

// NewEventChannel creates a channel of read functions that can be executed to
// read events from the source reader.
func NewEventChannel(ctx context.Context, sourceReader SourceReader) <-chan ReadFunc {
	channel := make(chan ReadFunc)
	eofChan := make(chan struct{})

	// Start a goroutine to manage the channel
	go func() {
		defer close(channel)

		for {
			select {
			case <-ctx.Done():
				return
			case <-eofChan:
				return
			default:
				// Send a read function for execution
				channel <- func() ([][]byte, error) {
					events, err := sourceReader.ReadEvents()
					if errors.Is(err, ErrEndOfInput) {
						// Signal that we've reached the end of input
						close(eofChan)
						return events, err
					}
					return events, err
				}
			}
		}
	}()

	return channel
}
