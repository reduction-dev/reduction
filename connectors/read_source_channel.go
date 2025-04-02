package connectors

import (
	"context"
	"errors"
	"math"
	"time"
)

const (
	// initialBackoffDuration is the starting duration for exponential backoff
	initialBackoffDuration = 100 * time.Millisecond

	// maxBackoffDuration is the maximum duration for backoff
	maxBackoffDuration = 10 * time.Second
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

		// Track consecutive failures for backoff calculations
		consecutiveFailures := 0

		// Singal that the previous `ReadFunc` finished
		readComplete := make(chan struct{}, 1)
		readComplete <- struct{}{}

		for {
			// Wait for the previous read function to finish and possibly apply
			// a backoff.
			select {
			case <-ctx.Done():
				return
			case <-readComplete:
				if atEOI {
					return
				}
				backoff(ctx, consecutiveFailures)
			}

			select {
			case <-ctx.Done():
				return
			case channel <- func() ([][]byte, error) {
				defer func() { readComplete <- struct{}{} }()
				events, err := sourceReader.ReadEvents()
				if errors.Is(err, ErrEndOfInput) {
					// Stop sending read functions
					atEOI = true

					// Hide EOI error from the caller
					return events, nil
				}

				if err != nil && IsRetryable(err) {
					consecutiveFailures++

					// Return the error to the caller for logging
					return nil, err
				}

				// Reset backoff state on success or terminal error
				consecutiveFailures = 0
				return events, err
			}:
			}
		}
	}()

	return channel
}

// backoff sleeps for an increasingly longer duration as failures accumulate, up
// to a maximum duration.
func backoff(ctx context.Context, consecutiveFailures int) {
	if consecutiveFailures == 0 {
		return
	}

	// Calculate exponential backoff with a maximum limit
	factor := math.Pow(2, float64(consecutiveFailures))
	duration := min(time.Duration(float64(initialBackoffDuration)*factor), maxBackoffDuration)

	select {
	case <-ctx.Done():
		return
	case <-time.After(duration):
	}
}
