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

type JobConnectAdapter struct {
	job        *jobs.Job
	clientOpts []connect.ClientOption
}

func NewJobConnectHandler(job *jobs.Job, clientOpts ...connect.ClientOption) (path string, handler http.Handler) {
	adapter := &JobConnectAdapter{job: job, clientOpts: clientOpts}
	logger := slog.With("instanceID", "job")
	return jobpbconnect.NewJobHandler(adapter, connect.WithInterceptors(NewLoggingInterceptor(logger)))
}

func (l *JobConnectAdapter) SourceCheckpointComplete(ctx context.Context, req *connect.Request[snapshotpb.SourceCheckpoint]) (*connect.Response[jobpb.Empty], error) {
	return connect.NewResponse(&jobpb.Empty{}), l.job.HandleSourceCheckpointComplete(ctx, req.Msg)
}

func (l *JobConnectAdapter) OperatorCheckpointComplete(ctx context.Context, req *connect.Request[snapshotpb.OperatorCheckpoint]) (*connect.Response[jobpb.Empty], error) {
	err := l.job.HandleOperatorCheckpointComplete(ctx, req.Msg)
	return connect.NewResponse(&jobpb.Empty{}), err
}

func (l *JobConnectAdapter) RegisterOperator(ctx context.Context, req *connect.Request[jobpb.NodeIdentity]) (*connect.Response[jobpb.Empty], error) {
	w := NewWorkerOperatorClient(req.Msg, l.clientOpts...)
	if w.Host() == "" {
		panic("listener missing host")
	}
	l.job.HandleRegisterOperator(w)

	return connect.NewResponse(&jobpb.Empty{}), nil
}

func (l *JobConnectAdapter) DeregisterOperator(ctx context.Context, req *connect.Request[jobpb.NodeIdentity]) (*connect.Response[jobpb.Empty], error) {
	l.job.HandleDeregisterOperator(req.Msg)
	return connect.NewResponse(&jobpb.Empty{}), nil
}

func (l *JobConnectAdapter) RegisterSourceRunner(ctx context.Context, req *connect.Request[jobpb.NodeIdentity]) (*connect.Response[jobpb.Empty], error) {
	sr := NewSourceRunnerConnectClient(req.Msg, l.clientOpts...)
	l.job.HandleRegisterSourceRunner(sr)

	return connect.NewResponse(&jobpb.Empty{}), nil
}

func (l *JobConnectAdapter) DeregisterSourceRunner(ctx context.Context, req *connect.Request[jobpb.NodeIdentity]) (*connect.Response[jobpb.Empty], error) {
	l.job.HandleDeregisterSourceRunner(req.Msg)
	return connect.NewResponse(&jobpb.Empty{}), nil
}

var _ jobpbconnect.JobHandler = (*JobConnectAdapter)(nil)
