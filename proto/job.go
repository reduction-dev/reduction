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

func (u UnimplementedJob) RegisterOperator(context.Context, *jobpb.NodeIdentity) error {
	panic("unimplemented")
}

func (u UnimplementedJob) DeregisterOperator(context.Context, *jobpb.NodeIdentity) error {
	panic("unimplemented")
}

func (u UnimplementedJob) RegisterSourceRunner(context.Context, *jobpb.NodeIdentity) error {
	panic("unimplemented")
}

func (u UnimplementedJob) DeregisterSourceRunner(context.Context, *jobpb.NodeIdentity) error {
	panic("unimplemented")
}

func (u UnimplementedJob) OnSourceCheckpointComplete(context.Context, *snapshotpb.SourceCheckpoint) error {
	panic("unimplemented")
}

func (u UnimplementedJob) OperatorCheckpointComplete(context.Context, *snapshotpb.OperatorCheckpoint) error {
	panic("unimplemented")
}

var _ Job = UnimplementedJob{}

type NoopJob struct{}

func (n NoopJob) DeregisterOperator(ctx context.Context, identity *jobpb.NodeIdentity) error {
	return nil
}

func (n NoopJob) DeregisterSourceRunner(ctx context.Context, identity *jobpb.NodeIdentity) error {
	return nil
}

func (n NoopJob) OnSourceCheckpointComplete(ctx context.Context, checkpoint *snapshotpb.SourceCheckpoint) error {
	return nil
}

func (n NoopJob) OperatorCheckpointComplete(ctx context.Context, req *snapshotpb.OperatorCheckpoint) error {
	return nil
}

func (n NoopJob) RegisterOperator(ctx context.Context, identity *jobpb.NodeIdentity) error {
	return nil
}

func (n NoopJob) RegisterSourceRunner(ctx context.Context, identity *jobpb.NodeIdentity) error {
	return nil
}

var _ Job = NoopJob{}
