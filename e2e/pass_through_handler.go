package e2e

import (
	"context"
	"time"

	"reduction.dev/reduction-go/connectors/httpapi"
	"reduction.dev/reduction-go/connectors/kinesis"
	"reduction.dev/reduction-go/rxn"
)

func NewPassThroughHandler(sink *httpapi.Sink, topic string) *PassThroughHandler {
	return &PassThroughHandler{
		sink:  sink,
		topic: topic,
	}
}

type PassThroughHandler struct {
	sink  *httpapi.Sink
	topic string
}

func (h *PassThroughHandler) OnEvent(ctx context.Context, user *rxn.Subject, rawEvent []byte) error {
	h.sink.Collect(ctx, &httpapi.SinkEvent{
		Topic: h.topic,
		Data:  rawEvent,
	})
	return nil
}

func (h *PassThroughHandler) OnTimerExpired(ctx context.Context, user *rxn.Subject, timer time.Time) error {
	return nil
}

func KeyKinesisEventWithRawKeyAndZeroTimestamp(ctx context.Context, record *kinesis.Record) ([]rxn.KeyedEvent, error) {
	return []rxn.KeyedEvent{{
		Key:       record.Data,
		Timestamp: time.Unix(0, 0),
		Value:     record.Data,
	}}, nil
}

var _ rxn.OperatorHandler = (*PassThroughHandler)(nil)
