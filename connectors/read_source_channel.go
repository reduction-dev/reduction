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

type ReadSourceChannel struct {
	// The channel to read from
	C chan ReadFunc

	// The source reader to read from
	sourceReader SourceReader

	// signal to stop sending read functions
	cancel context.CancelFunc

	// Flag that the channel is already running
	started bool
}

func NewReadSourceChannel(sourceReader SourceReader) *ReadSourceChannel {
	return &ReadSourceChannel{
		C:            make(chan ReadFunc),
		sourceReader: sourceReader,
	}
}

// Start sends read functions over a channel that the caller invokes to
// read events from the source reader.
func (c *ReadSourceChannel) Start(ctx context.Context) {
	if c.started {
		return
	}
	c.started = true

	// Allow Stop to cancel the loop
	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel

	go func() {
		// Track if we've received end of input
		atEOI := false

		// Track consecutive failures for backoff calculations
		consecutiveFailures := 0

		// Signal that the previous `ReadFunc` finished
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
			case c.C <- func() ([][]byte, error) {
				defer func() { readComplete <- struct{}{} }()

				events, err := c.sourceReader.ReadEvents()
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
}

// Stop stops sending read functions over the channel. The channel can be
// Started again.
func (c *ReadSourceChannel) Stop() {
	c.started = false

	// Cancel the context to stop the loop
	if c.cancel != nil {
		c.cancel()
		c.cancel = nil
	}
}

func (c *ReadSourceChannel) Close() {
	if c.cancel != nil {
		c.cancel()
	}

	close(c.C)
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
