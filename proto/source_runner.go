package proto

import (
	"context"

	"reduction.dev/reduction/proto/workerpb"
)

type SourceRunner interface {
	Host() string
	ID() string
	Start(context.Context, *workerpb.StartSourceRunnerRequest) error
	StartCheckpoint(ctx context.Context, id uint64) error
}
