package proto

import (
	"context"

	"reduction.dev/reduction/proto/jobpb"
	"reduction.dev/reduction/proto/snapshotpb"
)

// The RPC methods for a job
type Job interface {
	RegisterSourceRunner(ctx context.Context, identity *jobpb.NodeIdentity) error
	DeregisterSourceRunner(ctx context.Context, identity *jobpb.NodeIdentity) error
	RegisterOperator(ctx context.Context, identity *jobpb.NodeIdentity) error
	DeregisterOperator(ctx context.Context, identity *jobpb.NodeIdentity) error
	OperatorCheckpointComplete(ctx context.Context, req *snapshotpb.OperatorCheckpoint) error
	OnSourceCheckpointComplete(ctx context.Context, checkpoint *snapshotpb.SourceCheckpoint) error
}

type UnimplementedJob struct{}

// RegisterOperator implements Job.
func (u UnimplementedJob) RegisterOperator(context.Context, *jobpb.NodeIdentity) error {
	panic("unimplemented")
}

// DeregisterOperator implements Job.
func (u UnimplementedJob) DeregisterOperator(context.Context, *jobpb.NodeIdentity) error {
	panic("unimplemented")
}

// RegisterSourceRunner implements Job.
func (u UnimplementedJob) RegisterSourceRunner(context.Context, *jobpb.NodeIdentity) error {
	panic("unimplemented")
}

// DeregisterSourceRunner implements Job.
func (u UnimplementedJob) DeregisterSourceRunner(context.Context, *jobpb.NodeIdentity) error {
	panic("unimplemented")
}

// OnSourceCheckpointComplete implements Job.
func (u UnimplementedJob) OnSourceCheckpointComplete(context.Context, *snapshotpb.SourceCheckpoint) error {
	panic("unimplemented")
}

// OperatorCheckpointComplete implements Job.
func (u UnimplementedJob) OperatorCheckpointComplete(context.Context, *snapshotpb.OperatorCheckpoint) error {
	panic("unimplemented")
}

var _ Job = UnimplementedJob{}
