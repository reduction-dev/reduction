package connectors

import (
	"context"
	"errors"
)

// ReadFunc is a function that reads events from the source and returns the
// results or an error.
type ReadFunc func() ([][]byte, error)

// NewReadSourceChannel sends read functions over a channel that the caller invokes to
// read events from the source reader.
func NewReadSourceChannel(ctx context.Context, sourceReader SourceReader) <-chan ReadFunc {
	channel := make(chan ReadFunc)

	// Start a goroutine to manage the channel
	go func() {
		defer close(channel)

		// Track if we've received end of input
		atEOI := false

		for !atEOI {
			select {
			case <-ctx.Done():
				return
			case channel <- func() ([][]byte, error) {
				events, err := sourceReader.ReadEvents()
				if errors.Is(err, ErrEndOfInput) {
					// Stop sending read functions
					atEOI = true

					// Hide EOI error from the caller
					return events, nil
				}
				return events, err
			}:
			}
		}
	}()

	return channel
}
