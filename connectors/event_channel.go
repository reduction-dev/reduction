package connectors

import (
	"context"
	"errors"
)

// NewEventChannel creates a channel that reads events from the source reader.
func NewEventChannel(ctx context.Context, sourceReader SourceReader) <-chan ReadResult {
	channel := make(chan ReadResult)
	go func() {
		defer close(channel)

		for {
			select {
			case <-ctx.Done():
				return
			default:
				events, err := sourceReader.ReadEvents()
				select {
				case <-ctx.Done():
					return
				case channel <- ReadResult{
					Events: events,
					Err:    err,
				}:
					if errors.Is(err, ErrEndOfInput) {
						return
					}
				}
			}
		}
	}()

	return channel
}
