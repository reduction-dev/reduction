package proto

import (
	"context"

	"reduction.dev/reduction/proto/jobpb"
	"reduction.dev/reduction/proto/workerpb"
)

type Operator interface {
	ID() string
	Host() string
	HandleEventBatch(ctx context.Context, batch []*workerpb.Event) error
	Start(ctx context.Context, req *workerpb.StartOperatorRequest) error
	UpdateRetainedCheckpoints(ctx context.Context, ids []uint64) error
}

type OperatorFactory func(senderID string, node *jobpb.NodeIdentity) Operator
