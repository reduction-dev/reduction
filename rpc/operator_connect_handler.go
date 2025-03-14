package rpc

import (
	"context"
	"net/http"

	"reduction.dev/reduction/config"
	"reduction.dev/reduction/proto/workerpb"
	workerpbconnect "reduction.dev/reduction/proto/workerpb/workerpbconnect"
	"reduction.dev/reduction/workers/operator"

	"connectrpc.com/connect"
)

type OperatorConnectHandler struct {
	operator *operator.Operator
}

func NewOperatorConnectHandler(operator *operator.Operator) (string, http.Handler) {
	l := &OperatorConnectHandler{operator: operator}
	return workerpbconnect.NewOperatorHandler(l, connect.WithInterceptors(NewLoggingInterceptor(operator.Logger)))
}

// HandleEventBatch implements workerpbconnect.OperatorHandler.
func (l *OperatorConnectHandler) HandleEventBatch(ctx context.Context, req *connect.Request[workerpb.HandleEventBatchRequest]) (*connect.Response[workerpb.Empty], error) {
	senderID := req.Msg.SenderId
	for _, event := range req.Msg.Events {
		if err := l.operator.HandleEvent(ctx, senderID, event); err != nil {
			return nil, err
		}
	}

	return connect.NewResponse(&workerpb.Empty{}), nil
}

func (l *OperatorConnectHandler) Start(ctx context.Context, req *connect.Request[workerpb.StartOperatorRequest]) (*connect.Response[workerpb.Empty], error) {
	if len(req.Msg.Sinks) != 1 {
		panic("exactly one Sink config required")
	}
	sink := config.NewSinkFromProto(req.Msg.GetSinks()[0])
	err := l.operator.HandleStart(ctx, req.Msg, sink)
	return connect.NewResponse(&workerpb.Empty{}), err
}

func (l *OperatorConnectHandler) UpdateRetainedCheckpoints(ctx context.Context, req *connect.Request[workerpb.UpdateRetainedCheckpointsRequest]) (*connect.Response[workerpb.Empty], error) {
	return connect.NewResponse(&workerpb.Empty{}), l.operator.HandleRemoveCheckpoints(ctx, req.Msg)
}

var _ workerpbconnect.OperatorHandler = (*OperatorConnectHandler)(nil)
