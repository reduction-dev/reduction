package e2e

import (
	"context"
	"time"

	"reduction.dev/reduction-go/connectors"
	"reduction.dev/reduction-go/rxn"
)

func NewPassThroughHandler(sinkID string, topic string) *PassThroughHandler {
	return &PassThroughHandler{
		sink:  &connectors.HTTPAPISink{ID: sinkID},
		topic: topic,
	}
}

type PassThroughHandler struct {
	sink  *connectors.HTTPAPISink
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

func (h *PassThroughHandler) KeyEvent(ctx context.Context, rawEvent []byte) ([]rxn.KeyedEvent, error) {
	return []rxn.KeyedEvent{{
		Key:       rawEvent,
		Timestamp: time.Unix(0, 0),
		Value:     rawEvent,
	}}, nil
}

var _ rxn.Handler = (*PassThroughHandler)(nil)
