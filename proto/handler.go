package proto

import (
	"context"

	"reduction.dev/reduction-handler/handlerpb"
)

// The user's handler code for processing events
type Handler interface {
	OnEvent(ctx context.Context, req *handlerpb.OnEventRequest) (*handlerpb.HandlerResponse, error)
	OnTimerExpired(ctx context.Context, req *handlerpb.OnTimerExpiredRequest) (*handlerpb.HandlerResponse, error)
	KeyEvent(ctx context.Context, req *handlerpb.KeyEventRequest) (*handlerpb.KeyEventResponse, error)
}
