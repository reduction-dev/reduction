package operator

import (
	"iter"
	"maps"
	"time"

	"reduction.dev/reduction/proto/workerpb"
	"reduction.dev/reduction/util/iteru"
)

// TimerRegistry stores new timers and retrieves then when advancing the
// watermark.
type TimerRegistry struct {
	// The DKV db used to store timers.
	store *TimerStore

	// Keep track of upstreams because the effective watermark in the operator
	// should be the minimum of the upstream watermarks
	upstreams map[string]time.Time

	// Cache the current watermark, which is the minimum watermark of all
	// upstreams.
	watermark time.Time
}

func NewTimerRegistry(store *TimerStore, srIDs []string) *TimerRegistry {
	upstreams := make(map[string]time.Time)
	for _, id := range srIDs {
		upstreams[id] = time.Unix(0, 0)
	}

	return &TimerRegistry{
		upstreams: upstreams,
		store:     store,
	}
}

func (r *TimerRegistry) SetTimer(key []byte, t time.Time) {
	// Setting a timer on or before the composite watermark is a noop.
	if !r.watermark.Before(t) {
		return
	}

	r.store.Put(key, t)
}

// AdvanceWatermark registers a watermark and returns a list of keyed timer
// values that are due for processing.
func (r *TimerRegistry) AdvanceWatermark(senderID string, wm *workerpb.Watermark) iter.Seq2[[]byte, time.Time] {
	r.upstreams[senderID] = wm.Timestamp.AsTime()
	compositeWatermark := iteru.MinFunc(maps.Values(r.upstreams), time.Time.Compare)
	r.watermark = compositeWatermark

	return func(yield func([]byte, time.Time) bool) {
		for {
			timer, ok := r.store.GetEarliest()
			if !ok {
				break
			}

			// Stop if the watermark hasn't reached the earliest timer.
			if timer.Timestamp.After(compositeWatermark) {
				break
			}

			r.store.Delete(timer)
			if !yield(timer.Key, timer.Timestamp) {
				return
			}
		}
	}
}
