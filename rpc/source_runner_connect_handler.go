package rpc

import (
	"context"
	"log/slog"
	"net/http"

	"reduction.dev/reduction/proto/workerpb"
	"reduction.dev/reduction/proto/workerpb/workerpbconnect"
	"reduction.dev/reduction/workers/sourcerunner"

	"connectrpc.com/connect"
)

type SourceRunnerConnectHandler struct {
	sourceRunner *sourcerunner.SourceRunner
}

func NewSourceRunnerConnectHandler(sourceRunner *sourcerunner.SourceRunner) (string, http.Handler) {
	h := &SourceRunnerConnectHandler{sourceRunner: sourceRunner}
	return workerpbconnect.NewSourceRunnerHandler(h, connect.WithInterceptors(NewLoggingInterceptor(sourceRunner.Logger, slog.LevelDebug)))
}

func (s *SourceRunnerConnectHandler) Deploy(ctx context.Context, req *connect.Request[workerpb.DeploySourceRunnerRequest]) (*connect.Response[workerpb.Empty], error) {
	err := s.sourceRunner.HandleDeploy(ctx, req.Msg)
	return connect.NewResponse(&workerpb.Empty{}), err
}

func (s *SourceRunnerConnectHandler) StartCheckpoint(ctx context.Context, req *connect.Request[workerpb.StartCheckpointRequest]) (*connect.Response[workerpb.Empty], error) {
	s.sourceRunner.HandleStartCheckpoint(ctx, req.Msg.CheckpointId)
	return connect.NewResponse(&workerpb.Empty{}), nil
}

func (s *SourceRunnerConnectHandler) AssignSplits(ctx context.Context, req *connect.Request[workerpb.AssignSplitsRequest]) (*connect.Response[workerpb.Empty], error) {
	err := s.sourceRunner.HandleAssignSplits(req.Msg.Splits)
	return connect.NewResponse(&workerpb.Empty{}), err
}

var _ workerpbconnect.SourceRunnerHandler = (*SourceRunnerConnectHandler)(nil)
