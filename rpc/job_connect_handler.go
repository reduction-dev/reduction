package rpc

import (
	"context"
	"log/slog"
	"net/http"

	"reduction.dev/reduction/jobs"

	"reduction.dev/reduction/proto/jobpb"
	"reduction.dev/reduction/proto/jobpb/jobpbconnect"
	"reduction.dev/reduction/proto/snapshotpb"

	"connectrpc.com/connect"
)

type JobConnectHandler struct {
	job             *jobs.Job
	opClientFactory OperatorClientFactory
	srClientFactory SourceRunnerClientFactory
}

func NewJobConnectHandler(job *jobs.Job, srClientFactory SourceRunnerClientFactory, opClientFactory OperatorClientFactory) (path string, handler http.Handler) {
	h := &JobConnectHandler{job: job, opClientFactory: opClientFactory, srClientFactory: srClientFactory}
	logger := slog.With("instanceID", "job")
	return jobpbconnect.NewJobHandler(h, connect.WithInterceptors(NewLoggingInterceptor(logger)))
}

func (l *JobConnectHandler) SourceCheckpointComplete(ctx context.Context, req *connect.Request[snapshotpb.SourceCheckpoint]) (*connect.Response[jobpb.Empty], error) {
	return connect.NewResponse(&jobpb.Empty{}), l.job.HandleSourceCheckpointComplete(ctx, req.Msg)
}

func (l *JobConnectHandler) OperatorCheckpointComplete(ctx context.Context, req *connect.Request[snapshotpb.OperatorCheckpoint]) (*connect.Response[jobpb.Empty], error) {
	err := l.job.HandleOperatorCheckpointComplete(ctx, req.Msg)
	return connect.NewResponse(&jobpb.Empty{}), err
}

func (l *JobConnectHandler) RegisterOperator(ctx context.Context, req *connect.Request[jobpb.NodeIdentity]) (*connect.Response[jobpb.Empty], error) {
	op := l.opClientFactory(req.Msg)
	l.job.HandleRegisterOperator(op)

	return connect.NewResponse(&jobpb.Empty{}), nil
}

func (l *JobConnectHandler) DeregisterOperator(ctx context.Context, req *connect.Request[jobpb.NodeIdentity]) (*connect.Response[jobpb.Empty], error) {
	l.job.HandleDeregisterOperator(req.Msg)
	return connect.NewResponse(&jobpb.Empty{}), nil
}

func (l *JobConnectHandler) RegisterSourceRunner(ctx context.Context, req *connect.Request[jobpb.NodeIdentity]) (*connect.Response[jobpb.Empty], error) {
	sr := l.srClientFactory(req.Msg)
	l.job.HandleRegisterSourceRunner(sr)

	return connect.NewResponse(&jobpb.Empty{}), nil
}

func (l *JobConnectHandler) DeregisterSourceRunner(ctx context.Context, req *connect.Request[jobpb.NodeIdentity]) (*connect.Response[jobpb.Empty], error) {
	l.job.HandleDeregisterSourceRunner(req.Msg)
	return connect.NewResponse(&jobpb.Empty{}), nil
}

var _ jobpbconnect.JobHandler = (*JobConnectHandler)(nil)
