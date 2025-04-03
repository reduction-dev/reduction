package proto

import (
	"context"

	"reduction.dev/reduction/proto/jobpb"
	"reduction.dev/reduction/proto/workerpb"
)

type SourceRunner interface {
	Host() string
	ID() string
	Start(context.Context, *workerpb.StartSourceRunnerRequest) error
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

func (u *UnimplementedSourceRunner) Start(context.Context, *workerpb.StartSourceRunnerRequest) error {
	panic("unimplemented")
}

func (u *UnimplementedSourceRunner) StartCheckpoint(ctx context.Context, id uint64) error {
	panic("unimplemented")
}

var _ SourceRunner = (*UnimplementedSourceRunner)(nil)
