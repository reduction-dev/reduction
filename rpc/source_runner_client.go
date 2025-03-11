package rpc

import (
	"context"
	"log/slog"

	"connectrpc.com/connect"
	"reduction.dev/reduction/proto"
	"reduction.dev/reduction/proto/jobpb"
	"reduction.dev/reduction/proto/workerpb"
	workerpbconnect "reduction.dev/reduction/proto/workerpb/workerpbconnect"
)

type SourceRunnerConnectClient struct {
	host               string
	id                 string
	sourceRunnerClient workerpbconnect.SourceRunnerClient
}

func NewSourceRunnerConnectClient(identity *jobpb.NodeIdentity, opts ...connect.ClientOption) *SourceRunnerConnectClient {
	if identity.Host == "" {
		panic("missing host")
	}
	sourceRunnerClient := workerpbconnect.NewSourceRunnerClient(NewHTTPClient("source_runner", slog.Default()), "http://"+identity.Host, opts...)
	return &SourceRunnerConnectClient{
		host:               identity.Host,
		id:                 identity.Id,
		sourceRunnerClient: sourceRunnerClient,
	}
}

func (c *SourceRunnerConnectClient) Start(ctx context.Context, req *workerpb.StartSourceRunnerRequest) error {
	_, err := c.sourceRunnerClient.Start(ctx, connect.NewRequest(req))
	return err
}

func (c *SourceRunnerConnectClient) StartCheckpoint(ctx context.Context, id uint64) error {
	_, err := c.sourceRunnerClient.StartCheckpoint(ctx, connect.NewRequest(&workerpb.StartCheckpointRequest{
		CheckpointId: id,
	}))
	return err
}

func (c *SourceRunnerConnectClient) ID() string {
	return c.id
}

func (c *SourceRunnerConnectClient) Host() string {
	return c.host
}

var _ proto.SourceRunner = (*SourceRunnerConnectClient)(nil)

type SourceRunnerClientFactory func(node *jobpb.NodeIdentity) *SourceRunnerConnectClient
