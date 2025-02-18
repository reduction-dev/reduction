package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"time"

	"reduction.dev/reduction-go/connectors"
	"reduction.dev/reduction-go/connectors/httpapi"
	"reduction.dev/reduction-go/jobs"
	"reduction.dev/reduction-go/rxn"
)

type UserEvent struct {
	UserID    string
	Timestamp time.Time
}

func NewUserEventFromBytes(b []byte) (*UserEvent, error) {
	decoder := json.NewDecoder(bytes.NewBuffer(b))
	var event UserEvent
	err := decoder.Decode(&event)
	if err != nil {
		return nil, err
	}

	return &event, nil
}

// The e2e egress event
type CountInWindowEgressEvent struct {
	UserID string
	Count  int
}

type TimestampsCodec struct{}

func (c TimestampsCodec) DecodeValue(b []byte) ([]time.Time, error) {
	var ts []time.Time
	if err := json.Unmarshal(b, &ts); err != nil {
		return nil, err
	}
	return ts, nil
}

func (c TimestampsCodec) EncodeValue(value []time.Time) ([]byte, error) {
	return json.Marshal(value)
}

var _ rxn.ValueStateCodec[[]time.Time] = TimestampsCodec{}

type CountInWindowHandler struct {
	sink           connectors.SinkRuntime[*httpapi.SinkRecord]
	timestampsSpec rxn.ValueSpec[[]time.Time]
}

func NewCountInWindowHandler(sink connectors.SinkRuntime[*httpapi.SinkRecord], op *jobs.Operator) *CountInWindowHandler {
	return &CountInWindowHandler{
		timestampsSpec: rxn.NewValueSpec(op, "timestamps", TimestampsCodec{}),
		sink:           sink,
	}
}

func KeyEvent(ctx context.Context, rawEvent []byte) ([]rxn.KeyedEvent, error) {
	event, err := NewUserEventFromBytes(rawEvent)
	if err != nil {
		return nil, err
	}
	return []rxn.KeyedEvent{{
		Key:       []byte(event.UserID),
		Timestamp: event.Timestamp,
		Value:     rawEvent,
	}}, nil
}

func (h *CountInWindowHandler) OnEvent(ctx context.Context, subject *rxn.Subject, event rxn.KeyedEvent) error {
	userEvent, err := NewUserEventFromBytes(event.Value)
	if err != nil {
		return err
	}

	timestamps := h.timestampsSpec.StateFor(subject)
	ts := timestamps.Value()
	if ts == nil {
		ts = []time.Time{}
	}

	ts = append(ts, userEvent.Timestamp)
	timestamps.Set(ts)

	// Set timer for the next window
	subject.SetTimer(userEvent.Timestamp.Add(time.Millisecond).Round(time.Millisecond * 2))
	return nil
}

func (h *CountInWindowHandler) OnTimerExpired(ctx context.Context, subject *rxn.Subject, timer time.Time) error {
	timestamps := h.timestampsSpec.StateFor(subject)
	ts := timestamps.Value()
	if ts == nil {
		return nil
	}

	var closingTS []time.Time
	var remainingTS []time.Time
	for _, t := range ts {
		if !t.After(timer) {
			closingTS = append(closingTS, t)
		} else {
			remainingTS = append(remainingTS, t)
		}
	}

	timestamps.Set(remainingTS)

	egressEvent := CountInWindowEgressEvent{
		UserID: string(subject.Key()),
		Count:  len(closingTS),
	}

	egressEventJSON, err := json.Marshal(egressEvent)
	if err != nil {
		return err
	}
	h.sink.Collect(ctx, &httpapi.SinkRecord{
		Topic: "egress-events",
		Data:  egressEventJSON,
	})

	return nil
}
