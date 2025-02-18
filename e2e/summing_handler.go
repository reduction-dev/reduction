package e2e

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"reduction.dev/reduction-go/connectors"
	"reduction.dev/reduction-go/connectors/httpapi"
	"reduction.dev/reduction-go/jobs"
	"reduction.dev/reduction-go/rxn"
)

type SummingHandler struct {
	sink    connectors.SinkRuntime[*httpapi.SinkRecord]
	topic   string
	sumSpec rxn.ValueSpec[int64]
}

func NewSummingHandler(sink connectors.SinkRuntime[*httpapi.SinkRecord], topic string, op *jobs.Operator) *SummingHandler {
	return &SummingHandler{
		sink:    sink,
		topic:   topic,
		sumSpec: rxn.NewValueSpec(op, "sum", rxn.ScalarCodec[int64]{}),
	}
}

func (h *SummingHandler) OnEvent(ctx context.Context, subject *rxn.Subject, event rxn.KeyedEvent) error {
	var eventInt int64
	reader := bytes.NewReader(event.Value)
	err := binary.Read(reader, binary.BigEndian, &eventInt)
	if err != nil && err != io.EOF {
		return fmt.Errorf("reading rawEvent (data: %v): %w", event.Value, err)
	}

	sumState := h.sumSpec.StateFor(subject)
	nextSum := sumState.Value() + eventInt
	sumState.Set(nextSum)

	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.BigEndian, nextSum); err != nil {
		return err
	}

	h.sink.Collect(ctx, &httpapi.SinkRecord{
		Topic: h.topic,
		Data:  buf.Bytes(),
	})
	return nil
}

func (h *SummingHandler) OnTimerExpired(ctx context.Context, subject *rxn.Subject, timer time.Time) error {
	return nil
}

func KeyEventWithUniformKeyAndZeroTimestamp(ctx context.Context, rawEvent []byte) ([]rxn.KeyedEvent, error) {
	return []rxn.KeyedEvent{{
		Key:       []byte("static"),
		Timestamp: time.Unix(0, 0),
		Value:     rawEvent,
	}}, nil
}
