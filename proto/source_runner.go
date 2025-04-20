package proto

import (
	"context"

	"reduction.dev/reduction/proto/jobpb"
	"reduction.dev/reduction/proto/workerpb"
)

type SourceRunner interface {
	Host() string
	ID() string
	Deploy(context.Context, *workerpb.DeploySourceRunnerRequest) error
	AssignSplits(context.Context, []*workerpb.SourceSplit) error
	StartCheckpoint(ctx context.Context, id uint64) error
}

type SourceRunnerFactory func(node *jobpb.NodeIdentity) SourceRunner

type UnimplementedSourceRunner struct{}

func (u *UnimplementedSourceRunner) Host() string {
	panic("unimplemented")
}

func (u *UnimplementedSourceRunner) ID() string {
	panic("unimplemented")
}

func (u *UnimplementedSourceRunner) Deploy(context.Context, *workerpb.DeploySourceRunnerRequest) error {
	panic("unimplemented")
}

func (u *UnimplementedSourceRunner) AssignSplits(context.Context, []*workerpb.SourceSplit) error {
	panic("unimplemented")
}

func (u *UnimplementedSourceRunner) StartCheckpoint(ctx context.Context, id uint64) error {
	panic("unimplemented")
}

var _ SourceRunner = (*UnimplementedSourceRunner)(nil)
