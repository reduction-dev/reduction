package wmark

import "time"

// A watermarker keeps track of the time after which we expect no future events.
// As events arrive it advances the watermark. Callers can request the current
// watermark.
type Watermarker struct {
	maxTimestamp    time.Time
	allowedLateness time.Duration
}

// As events arrive, keep track of the latest time we've seen.
func (w *Watermarker) AdvanceTime(eventTimestamp time.Time) {
	if eventTimestamp.After(w.maxTimestamp) {
		w.maxTimestamp = eventTimestamp
	}
}

func (w *Watermarker) CurrentWatermark() time.Time {
	return w.maxTimestamp.Add(-(w.allowedLateness + time.Nanosecond))
}
