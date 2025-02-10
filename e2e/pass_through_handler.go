package e2e

import (
	"context"
	"time"

	"reduction.dev/reduction-go/connectors"
	"reduction.dev/reduction-go/rxn"
)

func NewPassThroughHandler(sink connectors.SinkRuntime[*connectors.HTTPSinkEvent], topic string) *PassThroughHandler {
	return &PassThroughHandler{
		sink:  sink,
		topic: topic,
	}
}

type PassThroughHandler struct {
	sink  connectors.SinkRuntime[*connectors.HTTPSinkEvent]
	topic string
}

func (h *PassThroughHandler) OnEvent(ctx context.Context, user *rxn.Subject, rawEvent []byte) error {
	h.sink.Collect(ctx, &connectors.HTTPSinkEvent{
		Topic: h.topic,
		Data:  rawEvent,
	})
	return nil
}

func (h *PassThroughHandler) OnTimerExpired(ctx context.Context, user *rxn.Subject, timer time.Time) error {
	return nil
}

// TODO: Remove
func (h *PassThroughHandler) KeyEvent(ctx context.Context, rawEvent []byte) ([]rxn.KeyedEvent, error) {
	return []rxn.KeyedEvent{{
		Key:       rawEvent,
		Timestamp: time.Unix(0, 0),
		Value:     rawEvent,
	}}, nil
}

func KeyKinesisEventWithRawKeyAndZeroTimestamp(ctx context.Context, record *connectors.KinesisRecord) ([]rxn.KeyedEvent, error) {
	return []rxn.KeyedEvent{{
		Key:       record.Data,
		Timestamp: time.Unix(0, 0),
		Value:     record.Data,
	}}, nil
}

var _ rxn.Handler = (*PassThroughHandler)(nil)
