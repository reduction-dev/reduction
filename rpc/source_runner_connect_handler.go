package rpc

import (
	"context"
	"net/http"

	"reduction.dev/reduction/proto"
	"reduction.dev/reduction/proto/workerpb"
	"reduction.dev/reduction/proto/workerpb/workerpbconnect"
	"reduction.dev/reduction/workers/sourcerunner"

	"connectrpc.com/connect"
)

type SourceRunnerConnectAdapter struct {
	sourceRunner *sourcerunner.SourceRunner
}

func NewSourceRunnerConnectHandler(sourceRunner *sourcerunner.SourceRunner) (string, http.Handler) {
	l := &SourceRunnerConnectAdapter{sourceRunner: sourceRunner}
	return workerpbconnect.NewSourceRunnerHandler(l, connect.WithInterceptors(NewLoggingInterceptor(sourceRunner.Logger)))
}

func (s *SourceRunnerConnectAdapter) Start(ctx context.Context, req *connect.Request[workerpb.StartSourceRunnerRequest]) (*connect.Response[workerpb.Empty], error) {
	ops := make([]proto.Operator, len(req.Msg.Operators))
	for i, op := range req.Msg.Operators {
		ops[i] = NewWorkerOperatorClient(op)
	}

	err := s.sourceRunner.HandleStart(ctx, ops, req.Msg)
	return connect.NewResponse(&workerpb.Empty{}), err
}

func (s *SourceRunnerConnectAdapter) StartCheckpoint(ctx context.Context, req *connect.Request[workerpb.StartCheckpointRequest]) (*connect.Response[workerpb.Empty], error) {
	s.sourceRunner.HandleStartCheckpoint(ctx, req.Msg.CheckpointId)
	return connect.NewResponse(&workerpb.Empty{}), nil
}

var _ workerpbconnect.SourceRunnerHandler = (*SourceRunnerConnectAdapter)(nil)
