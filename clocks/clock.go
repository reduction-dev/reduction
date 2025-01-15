package clocks

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type Clock interface {
	Now() time.Time
	Every(d time.Duration, fn func(*EveryContext), label string) *Ticker
}

type Ticker struct {
	cancel  context.CancelFunc
	trigger func()
}

func (t *Ticker) Stop() {
	t.cancel()
}

// Immediately trigger the configured function, resetting the time before the
// next tick.
func (t *Ticker) Trigger() {
	t.trigger()
}

type EveryContext struct {
	retryIn time.Duration
}

func (tc *EveryContext) RetryIn(d time.Duration) {
	tc.retryIn = d
}

type SystemClock struct{}

func (c *SystemClock) Every(d time.Duration, fn func(*EveryContext), _label string) *Ticker {
	// Periodic ticker
	ticker := time.NewTicker(d)

	// Context used to stop all future fn calls
	ctx, cancel := context.WithCancel(context.Background())

	// Context to get retry signal from caller
	tc := &EveryContext{}

	tick := func() {
		fn(tc)

		// If fn set retryIn
		for tc.retryIn != 0 {
			// Create a timer for the retry
			retryTimer := time.NewTimer(tc.retryIn)

			select {
			case <-retryTimer.C:
				// Reset retryIn for the next call
				tc.retryIn = 0
				fn(tc)

				// If retryIn is still 0, we're exiting so reset the ticker
				if tc.retryIn == 0 {
					ticker.Reset(d)
				}
			case <-ctx.Done():
				return
			}
		}
	}

	go func() {
		for {
			select {
			case <-ticker.C:
				tick()
			case <-ctx.Done():
				return
			}
		}
	}()

	return &Ticker{
		cancel: cancel,
		trigger: func() {
			tick()
			ticker.Reset(d)
		},
	}
}

func NewSystemClock() *SystemClock {
	return &SystemClock{}
}

func (c *SystemClock) Now() time.Time {
	return time.Now()
}

var _ Clock = (*SystemClock)(nil)

type FrozenClock struct {
	now        time.Time
	everyFuncs map[string]func()
	mu         *sync.Mutex
}

// Every for FrozenClock is a no-op
func (c *FrozenClock) Every(d time.Duration, fn func(*EveryContext), label string) *Ticker {
	c.mu.Lock()
	defer c.mu.Unlock()

	ctx := &EveryContext{}
	c.everyFuncs[label] = func() {
		fn(ctx)
	}

	return &Ticker{
		cancel:  func() {},
		trigger: c.everyFuncs[label],
	}
}

func NewFrozenClock() *FrozenClock {
	return &FrozenClock{
		now:        time.Unix(0, 0),
		everyFuncs: make(map[string]func()),
		mu:         &sync.Mutex{},
	}
}

func (c *FrozenClock) Now() time.Time {
	return c.now
}

func (c *FrozenClock) Advance(d time.Duration) {
	c.now = c.now.Add(d)
}

func (c *FrozenClock) TickEvery(label string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	fn := c.everyFuncs[label]
	if fn == nil {
		panic(fmt.Sprintf("FrozenClock has no `every` func registered for label %s", label))
	}
	fn()
}

var _ Clock = (*FrozenClock)(nil)
