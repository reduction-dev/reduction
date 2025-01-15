package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"time"

	"reduction.dev/reduction-go/connectors"
	"reduction.dev/reduction-go/rxn"
	"reduction.dev/reduction/proto/e2epb"

	"google.golang.org/protobuf/proto"
)

func NewCountInWindowHandler() *CountInWindowHandler {
	return &CountInWindowHandler{
		sink: &connectors.HTTPAPISink{ID: "sink"},
	}
}

type CountInWindowHandler struct {
	sink *connectors.HTTPAPISink
}

var _ rxn.Handler = (*CountInWindowHandler)(nil)

// Defining the state item

type TimeList struct {
	timestamps []time.Time
}

func (tl *TimeList) Load(entries []rxn.StateEntry) error {
	// Use one entry to load the list data
	var entry rxn.StateEntry
	if len(entries) > 0 {
		entry = entries[0]
	}

	var protoMessage e2epb.TimestampList
	if err := proto.Unmarshal(entry.Value, &protoMessage); err != nil {
		return err
	}

	tl.timestamps = make([]time.Time, len(protoMessage.Timestamps))
	for i, t := range protoMessage.Timestamps {
		tl.timestamps[i] = time.UnixMilli(t)
	}

	return nil

}

func (tl *TimeList) Mutations() ([]rxn.StateMutation, error) {
	ts := make([]int64, len(tl.timestamps))
	for i, t := range tl.timestamps {
		ts[i] = t.UnixMilli()
	}

	protoMessage := e2epb.TimestampList{
		Timestamps: ts,
	}
	data, err := proto.Marshal(&protoMessage)
	if err != nil {
		return nil, err
	}

	return []rxn.StateMutation{&rxn.PutMutation{
		Key:   []byte(tl.Name()),
		Value: data,
	}}, nil
}

func (tl *TimeList) Name() string {
	return "time-list"
}

func (tl *TimeList) Marshal() ([]byte, error) {
	ts := make([]int64, len(tl.timestamps))
	for i, t := range tl.timestamps {
		ts[i] = t.UnixMilli()
	}

	protoMessage := e2epb.TimestampList{
		Timestamps: ts,
	}
	return proto.Marshal(&protoMessage)
}

func (tl *TimeList) Unmarshal(bs []byte) error {
	var protoMessage e2epb.TimestampList
	err := proto.Unmarshal(bs, &protoMessage)
	if err != nil {
		return err
	}

	tl.timestamps = make([]time.Time, len(protoMessage.Timestamps))
	for i, t := range protoMessage.Timestamps {
		tl.timestamps[i] = time.UnixMilli(t)
	}

	return nil
}

var _ rxn.StateItem = (*TimeList)(nil)

// Defining the event being processed

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

func (h *CountInWindowHandler) KeyEvent(ctx context.Context, rawEvent []byte) ([]rxn.KeyedEvent, error) {
	event, err := NewUserEventFromBytes(rawEvent)
	return []rxn.KeyedEvent{{
		Key:       []byte(event.UserID),
		Timestamp: event.Timestamp,
		Value:     rawEvent,
	}}, err
}

func (h *CountInWindowHandler) OnEvent(ctx context.Context, user *rxn.Subject, rawEvent []byte) error {
	event, err := NewUserEventFromBytes(rawEvent)
	if err != nil {
		return err
	}

	var tsList TimeList
	err = user.State(&tsList)
	if err != nil {
		return err
	}

	tsList.timestamps = append(tsList.timestamps, event.Timestamp)
	user.UpdateState(&tsList)

	user.SetTimer(event.Timestamp.Add(time.Millisecond).Round(time.Millisecond * 2))

	return nil
}

func (h *CountInWindowHandler) OnTimerExpired(ctx context.Context, user *rxn.Subject, timer time.Time) error {
	var tsList TimeList
	err := user.State(&tsList)
	if err != nil {
		return err
	}

	var closingTSList []time.Time
	var remainingTSList []time.Time
	for _, t := range tsList.timestamps {
		if !t.After(timer) {
			closingTSList = append(closingTSList, t)
		} else {
			remainingTSList = append(remainingTSList, t)
		}
	}

	tsList.timestamps = remainingTSList
	user.UpdateState(&tsList)

	egressEvent := CountInWindowEgressEvent{UserID: string(user.Key()), Count: len(closingTSList)}

	egressEventJSON, err := json.Marshal(egressEvent)
	if err != nil {
		return err
	}
	h.sink.Collect(ctx, &connectors.HTTPSinkEvent{
		Topic: "egress-events",
		Data:  egressEventJSON,
	})

	return nil
}
