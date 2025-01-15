package rpc

import (
	"context"
	"net/http"

	"reduction.dev/reduction/jobs"

	"reduction.dev/reduction/proto/jobpb"
	"reduction.dev/reduction/proto/jobpb/jobpbconnect"

	"connectrpc.com/connect"
)

type JobUIConnectAdapter struct {
	job *jobs.Job
}

func NewJobUIConnectHandler(job *jobs.Job) (path string, handler http.Handler) {
	adapter := &JobUIConnectAdapter{job}
	return jobpbconnect.NewJobUIHandler(adapter)
}

func (l *JobUIConnectAdapter) CreateSavepoint(ctx context.Context, req *connect.Request[jobpb.Empty]) (*connect.Response[jobpb.CreateSavepointResponse], error) {
	id, err := l.job.HandleCreateSavepoint(ctx)
	return connect.NewResponse(&jobpb.CreateSavepointResponse{
		SavepointId: id,
	}), err
}

func (l *JobUIConnectAdapter) GetSavepoint(ctx context.Context, req *connect.Request[jobpb.GetSavepointRequest]) (*connect.Response[jobpb.GetSavepointResponse], error) {
	uri, err := l.job.HandleGetSavepointURI(ctx, req.Msg.SavepointId)
	return connect.NewResponse(&jobpb.GetSavepointResponse{
		Uri: uri,
	}), err
}

var _ jobpbconnect.JobUIHandler = (*JobUIConnectAdapter)(nil)
