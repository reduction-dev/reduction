package rpc

import (
	"context"
	"log/slog"

	jobpb "reduction.dev/reduction/proto/jobpb"
	jobconnect "reduction.dev/reduction/proto/jobpb/jobpbconnect"

	"connectrpc.com/connect"
)

type JobUIConnectClient struct {
	connClient jobconnect.JobUIClient
}

func NewJobUIConnectClient(host string, opts ...connect.ClientOption) *JobUIConnectClient {
	return &JobUIConnectClient{connClient: jobconnect.NewJobUIClient(NewHTTPClient("job_ui", slog.Default()), "http://"+host, opts...)}
}

func (c *JobUIConnectClient) CreateSavepoint(ctx context.Context) (uint64, error) {
	resp, err := c.connClient.CreateSavepoint(ctx, connect.NewRequest(&jobpb.Empty{}))
	if err != nil {
		return 0, err
	}
	return resp.Msg.SavepointId, nil
}

func (c *JobUIConnectClient) GetSavepoint(ctx context.Context, id uint64) (string, error) {
	resp, err := c.connClient.GetSavepoint(ctx, connect.NewRequest(&jobpb.GetSavepointRequest{
		SavepointId: id,
	}))
	if err != nil {
		return "", err
	}
	return resp.Msg.Uri, nil
}
