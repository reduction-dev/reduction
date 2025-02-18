package e2e

import (
	"context"
	"encoding/json"
	"time"

	"reduction.dev/reduction-go/connectors"
	"reduction.dev/reduction-go/connectors/httpapi"
	"reduction.dev/reduction-go/jobs"
	"reduction.dev/reduction-go/rxn"
)

type SlidingWindowOutput struct {
	Key      string `json:"key"`
	Sum      int    `json:"sum"`
	Interval string `json:"interval"`
}

type SlidingWindowEvent struct {
	Key       string    `json:"key"`
	Timestamp time.Time `json:"timestamp"`
}

type CountByMinuteCodec struct{}

func (c CountByMinuteCodec) DecodeValue(b []byte) (map[time.Time]int, error) {
	var buckets map[time.Time]int
	if err := json.Unmarshal(b, &buckets); err != nil {
		return nil, err
	}
	return buckets, nil
}

func (c CountByMinuteCodec) EncodeValue(value map[time.Time]int) ([]byte, error) {
	return json.Marshal(value)
}

var _ rxn.ValueStateCodec[map[time.Time]int] = CountByMinuteCodec{}

type SlidingWindowHandler struct {
	sink              connectors.SinkRuntime[*httpapi.SinkRecord]
	countByMinuteSpec rxn.ValueSpec[map[time.Time]int]
}

func NewSlidingWindowHandler(sink connectors.SinkRuntime[*httpapi.SinkRecord], op *jobs.Operator) *SlidingWindowHandler {
	return &SlidingWindowHandler{
		sink:              sink,
		countByMinuteSpec: rxn.NewValueSpec(op, "window-state", CountByMinuteCodec{}),
	}
}

func (h *SlidingWindowHandler) OnEvent(ctx context.Context, subject *rxn.Subject, keyedEvent rxn.KeyedEvent) error {
	var event SlidingWindowEvent
	if err := json.Unmarshal(keyedEvent.Value, &event); err != nil {
		return err
	}

	// Round to minute bucket
	minute := event.Timestamp.Truncate(time.Minute)

	countByMinute := h.countByMinuteSpec.StateFor(subject)
	state := countByMinute.Value()
	if state == nil {
		state = make(map[time.Time]int)
	}
	state[minute]++
	countByMinute.Set(state)

	// Set timer for next minute
	nextMinute := event.Timestamp.Truncate(time.Minute).Add(time.Minute)
	subject.SetTimer(nextMinute)

	return nil
}

func (h *SlidingWindowHandler) OnTimerExpired(ctx context.Context, subject *rxn.Subject, timer time.Time) error {
	countByMinute := h.countByMinuteSpec.StateFor(subject)
	state := countByMinute.Value()
	if state == nil {
		return nil
	}

	currentWindow := newWindow(timer.Truncate(time.Minute), time.Hour)
	sum := 0
	newState := make(map[time.Time]int)
	for ts, count := range state {
		if currentWindow.contains(ts) {
			sum += count
			newState[ts] = count
		} else if !currentWindow.isPast(ts) {
			newState[ts] = count
		}
	}
	countByMinute.Set(newState)

	// If we have any data in state don't emit or set any more timers
	if len(newState) == 0 {
		return nil
	}

	output := SlidingWindowOutput{
		Key:      string(subject.Key()),
		Sum:      sum,
		Interval: currentWindow.intervalString(),
	}
	data, err := json.Marshal(output)
	if err != nil {
		return err
	}
	h.sink.Collect(ctx, &httpapi.SinkRecord{
		Topic: "windows",
		Data:  data,
	})

	// Ensure a timer is fired on the next watermark in case there are no more events
	// for this subject.
	subject.SetTimer(rxn.Watermark(ctx).Add(time.Minute).Truncate(time.Minute))

	return nil
}

func KeySlidingWindowEvent(ctx context.Context, rawEvent []byte) ([]rxn.KeyedEvent, error) {
	var event SlidingWindowEvent
	if err := json.Unmarshal(rawEvent, &event); err != nil {
		return nil, err
	}

	return []rxn.KeyedEvent{{
		Key:       []byte(event.Key),
		Timestamp: event.Timestamp,
		Value:     rawEvent,
	}}, nil
}

type window struct {
	start time.Time
	end   time.Time
}

func newWindow(end time.Time, duration time.Duration) window {
	return window{
		start: end.Add(-duration),
		end:   end,
	}
}

func (w window) contains(t time.Time) bool {
	return !t.Before(w.start) && t.Before(w.end)
}

func (w window) isPast(t time.Time) bool {
	return t.Before(w.start)
}

func (w window) intervalString() string {
	return w.start.Format(time.RFC3339) + "/" + w.end.Format(time.RFC3339)
}
