package workerstest

import (
	"context"

	"reduction.dev/reduction/proto"
	"reduction.dev/reduction/proto/jobpb"
	"reduction.dev/reduction/proto/snapshotpb"
)

type DummyJob struct {
	proto.UnimplementedJob
	OperatorCheckpoint *snapshotpb.OperatorCheckpoint
}

func (d *DummyJob) RegisterOperator(context.Context, *jobpb.NodeIdentity) error {
	return nil
}

func (d *DummyJob) DeregisterOperator(context.Context, *jobpb.NodeIdentity) error {
	return nil
}

func (d *DummyJob) RegisterSourceRunner(context.Context, *jobpb.NodeIdentity) error {
	return nil
}

func (d *DummyJob) DeregisterSourceRunner(context.Context, *jobpb.NodeIdentity) error {
	return nil
}

func (d *DummyJob) OperatorCheckpointComplete(ctx context.Context, req *snapshotpb.OperatorCheckpoint) error {
	d.OperatorCheckpoint = req
	return nil
}

var _ proto.Job = (*DummyJob)(nil)
