package proto

import (
	"context"

	"reduction.dev/reduction/proto/workerpb"
)

type Operator interface {
	ID() string
	Host() string
	HandleEvent(ctx context.Context, event *workerpb.Event) error
	Start(ctx context.Context, req *workerpb.StartOperatorRequest) error
}
