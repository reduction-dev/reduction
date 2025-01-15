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

type OperatorConnectAdapter struct {
	operator *operator.Operator
}

func NewOperatorConnectHandler(operator *operator.Operator) (string, http.Handler) {
	l := &OperatorConnectAdapter{operator: operator}
	return workerpbconnect.NewOperatorHandler(l, connect.WithInterceptors(NewLoggingInterceptor(operator.Logger)))
}

func (l *OperatorConnectAdapter) HandleEvent(ctx context.Context, req *connect.Request[workerpb.HandleEventRequest]) (*connect.Response[workerpb.Empty], error) {
	if err := l.operator.HandleEvent(ctx, req.Msg); err != nil {
		return nil, err
	}

	return connect.NewResponse(&workerpb.Empty{}), nil
}

func (l *OperatorConnectAdapter) Start(ctx context.Context, req *connect.Request[workerpb.StartOperatorRequest]) (*connect.Response[workerpb.Empty], error) {
	if len(req.Msg.Sinks) != 1 {
		panic("exactly one Sink config required")
	}
	sink := config.NewSinkFromProto(req.Msg.GetSinks()[0])
	err := l.operator.HandleStart(ctx, req.Msg, sink)
	return connect.NewResponse(&workerpb.Empty{}), err
}

var _ workerpbconnect.OperatorHandler = (*OperatorConnectAdapter)(nil)
