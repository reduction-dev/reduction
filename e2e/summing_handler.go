package e2e

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"reduction.dev/reduction-go/connectors"
	"reduction.dev/reduction-go/rxn"
)

func NewSummingHandler(sinkID string, topic string) *SummingHandler {
	return &SummingHandler{
		sink:  &connectors.HTTPAPISink{ID: sinkID},
		topic: topic,
	}
}

type SummingHandler struct {
	sink  *connectors.HTTPAPISink
	topic string
}

func (h *SummingHandler) OnEvent(ctx context.Context, user *rxn.Subject, rawEvent []byte) error {
	var sum IntegerState
	if err := user.LoadState(&sum); err != nil {
		return fmt.Errorf("getting sum state: %w", err)
	}

	var eventInt int64
	reader := bytes.NewReader(rawEvent)
	err := binary.Read(reader, binary.LittleEndian, &eventInt)
	if err != nil && err != io.EOF {
		return fmt.Errorf("reading rawEvent (data: %v): %w", rawEvent, err)
	}

	nextSum := sum.Add(eventInt)
	user.UpdateState(nextSum)

	nextSumBytes, err := nextSum.Marshal()
	if err != nil {
		return err
	}

	h.sink.Collect(ctx, &connectors.HTTPSinkEvent{
		Topic: h.topic,
		Data:  nextSumBytes,
	})
	return nil
}

func (h *SummingHandler) OnTimerExpired(ctx context.Context, user *rxn.Subject, timer time.Time) error {
	return nil
}

func (h *SummingHandler) KeyEvent(ctx context.Context, rawEvent []byte) ([]rxn.KeyedEvent, error) {
	return []rxn.KeyedEvent{{
		Key:       []byte("static"),
		Timestamp: time.Unix(0, 0),
		Value:     rawEvent,
	}}, nil
}

var _ rxn.Handler = (*SummingHandler)(nil)

type IntegerState struct {
	value int64
}

// Load initialized IntegerState from the request payload
func (s *IntegerState) Load(entries []rxn.StateEntry) error {
	var entry rxn.StateEntry
	if len(entries) > 0 {
		entry = entries[0]
	}

	reader := bytes.NewReader(entry.Value)
	err := binary.Read(reader, binary.BigEndian, &s.value)
	if err == io.EOF {
		return nil
	}
	return err
}

// Mutations creates a list of mutations to synchronize the DB with this state item.
func (s *IntegerState) Mutations() ([]rxn.StateMutation, error) {
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.BigEndian, int64(s.value)); err != nil {
		return nil, err
	}

	return []rxn.StateMutation{&rxn.PutMutation{
		Key:   []byte(s.Name()),
		Value: buf.Bytes(),
	}}, nil
}

func (s *IntegerState) Marshal() ([]byte, error) {
	var buf bytes.Buffer
	err := binary.Write(&buf, binary.LittleEndian, int64(s.value))
	return buf.Bytes(), err
}

func (s *IntegerState) Name() string {
	return "IntegerState"
}

func (s *IntegerState) Unmarshal(data []byte) error {
	reader := bytes.NewReader(data)
	err := binary.Read(reader, binary.LittleEndian, &s.value)
	if err == io.EOF {
		return nil
	}
	return err
}

func (s *IntegerState) Add(i int64) *IntegerState {
	return &IntegerState{value: s.value + int64(i)}
}

var _ rxn.StateItem = (*IntegerState)(nil)
