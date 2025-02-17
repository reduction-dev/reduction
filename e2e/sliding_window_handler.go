package e2e

import (
	"context"
	"encoding/json"
	"time"

	"reduction.dev/reduction-go/connectors"
	"reduction.dev/reduction-go/connectors/httpapi"
	"reduction.dev/reduction-go/rxn"
)

type SlidingWindowHandler struct {
	sink connectors.SinkRuntime[*httpapi.SinkRecord]
}

type SlidingWindowOutput struct {
	Key      string `json:"key"`
	Sum      int    `json:"sum"`
	Interval string `json:"interval"`
}

type SlidingWindowEvent struct {
	Key       string    `json:"key"`
	Timestamp time.Time `json:"timestamp"`
}

func NewSlidingWindowHandler(sink connectors.SinkRuntime[*httpapi.SinkRecord]) *SlidingWindowHandler {
	return &SlidingWindowHandler{
		sink: sink,
	}
}

func (h *SlidingWindowHandler) OnEvent(ctx context.Context, user *rxn.Subject, keyedEvent rxn.KeyedEvent) error {
	var state SlidingWindowState
	if err := user.LoadState(&state); err != nil {
		return err
	}

	var event SlidingWindowEvent
	if err := json.Unmarshal(keyedEvent.Value, &event); err != nil {
		return err
	}

	// Round to minute bucket
	bucketTS := event.Timestamp.Truncate(time.Minute)

	if state.Buckets == nil {
		state.Buckets = make(map[time.Time]int)
	}
	state.Buckets[bucketTS]++

	// Register state usage before updating
	user.RegisterStateUse(state.Name(), func() ([]rxn.StateMutation, error) {
		return state.Mutations()
	})
	user.UpdateState(&state)

	// Set timer for next minute
	nextMin := event.Timestamp.Truncate(time.Minute).Add(time.Minute)
	user.SetTimer(nextMin)

	return nil
}

func (h *SlidingWindowHandler) OnTimerExpired(ctx context.Context, user *rxn.Subject, timer time.Time) error {
	var state SlidingWindowState
	if err := user.LoadState(&state); err != nil {
		return err
	}

	currentWindow := newWindow(timer.Truncate(time.Minute), time.Hour)
	sum := 0
	for ts := range state.Buckets {
		if currentWindow.contains(ts) {
			sum += state.Buckets[ts]
		} else if currentWindow.isPast(ts) {
			delete(state.Buckets, ts)
		}
	}

	// Register state usage before updating
	user.RegisterStateUse(state.Name(), func() ([]rxn.StateMutation, error) {
		return state.Mutations()
	})
	user.UpdateState(&state)

	// If we have no data don't emit or set any more timers
	if sum == 0 {
		return nil
	}

	output := SlidingWindowOutput{
		Key:      string(user.Key()),
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
	user.SetTimer(rxn.CurrentWatermark(ctx).Add(time.Minute).Truncate(time.Minute))

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

var _ rxn.OperatorHandler = (*SlidingWindowHandler)(nil)

type SlidingWindowState struct {
	Buckets map[time.Time]int `json:"buckets"`
}

func (s *SlidingWindowState) Load(entries []rxn.StateEntry) error {
	if len(entries) == 0 {
		s.Buckets = make(map[time.Time]int)
		return nil
	}
	return json.Unmarshal(entries[0].Value, s)
}

func (s *SlidingWindowState) Mutations() ([]rxn.StateMutation, error) {
	data, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	return []rxn.StateMutation{&rxn.PutMutation{
		Key:   []byte(s.Name()),
		Value: data,
	}}, nil
}

func (s *SlidingWindowState) Name() string {
	return "window-state"
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
