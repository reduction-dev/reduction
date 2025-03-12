package workerstest

import (
	"context"

	"reduction.dev/reduction-protocol/handlerpb"
	"reduction.dev/reduction/proto"
)

type RecordingHandler struct {
	// Records all ProcessEventBatch requests made to this handler
	ProcessEventBatchRequests []*handlerpb.ProcessEventBatchRequest
}

func (r *RecordingHandler) KeyEventBatch(ctx context.Context, events [][]byte) ([][]*handlerpb.KeyedEvent, error) {
	panic("unused by operators")
}

func (r *RecordingHandler) KeyEventResults() <-chan []*handlerpb.KeyedEvent {
	panic("unused by operators")
}

func (r *RecordingHandler) ProcessEventBatch(ctx context.Context, req *handlerpb.ProcessEventBatchRequest) (*handlerpb.ProcessEventBatchResponse, error) {
	r.ProcessEventBatchRequests = append(r.ProcessEventBatchRequests, req)

	// Return empty response since we're just recording
	return &handlerpb.ProcessEventBatchResponse{}, nil
}

var _ proto.Handler = (*RecordingHandler)(nil)
