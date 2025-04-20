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
	Deploy(ctx context.Context, req *workerpb.DeployOperatorRequest) error
	UpdateRetainedCheckpoints(ctx context.Context, ids []uint64) error
	NeedsTable(ctx context.Context, fileURI string) (bool, error)
}

type OperatorFactory func(senderID string, node *jobpb.NodeIdentity) Operator

type UnimplementedOperator struct{}

func (u *UnimplementedOperator) HandleEventBatch(ctx context.Context, batch []*workerpb.Event) error {
	panic("unimplemented")
}

func (u *UnimplementedOperator) Host() string {
	panic("unimplemented")
}

func (u *UnimplementedOperator) ID() string {
	panic("unimplemented")
}

func (u *UnimplementedOperator) NeedsTable(ctx context.Context, fileURI string) (bool, error) {
	panic("unimplemented")
}

func (u *UnimplementedOperator) Deploy(ctx context.Context, req *workerpb.DeployOperatorRequest) error {
	panic("unimplemented")
}

func (u *UnimplementedOperator) UpdateRetainedCheckpoints(ctx context.Context, ids []uint64) error {
	panic("unimplemented")
}

var _ Operator = (*UnimplementedOperator)(nil)
