package proto

import (
	"context"

	"reduction.dev/reduction-handler/handlerpb"
)

// The user's handler code for processing events
type Handler interface {
	ProcessEventBatch(ctx context.Context, req *handlerpb.ProcessEventBatchRequest) (*handlerpb.ProcessEventBatchResponse, error)
	KeyEventBatch(ctx context.Context, events [][]byte) ([][]*handlerpb.KeyedEvent, error)
}
