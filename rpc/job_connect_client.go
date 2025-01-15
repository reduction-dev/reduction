package rpc

import (
	"context"

	"reduction.dev/reduction/proto/jobpb"
	"reduction.dev/reduction/proto/jobpb/jobpbconnect"
	"reduction.dev/reduction/proto/snapshotpb"

	"connectrpc.com/connect"
)

func NewJobConnectClient(host string, opts ...connect.ClientOption) *JobConnectClient {
	client := jobpbconnect.NewJobClient(NewHTTPClient(), "http://"+host, opts...)
	return &JobConnectClient{
		connClient: client,
	}
}

type JobConnectClient struct {
	connClient jobpbconnect.JobClient
}

func (c *JobConnectClient) RegisterOperator(ctx context.Context, identity *jobpb.NodeIdentity) error {
	_, err := c.connClient.RegisterOperator(ctx, connect.NewRequest(identity))
	return err
}

func (c *JobConnectClient) DeregisterOperator(ctx context.Context, op *jobpb.NodeIdentity) error {
	_, err := c.connClient.DeregisterOperator(ctx, connect.NewRequest(op))
	return err
}

func (c *JobConnectClient) RegisterSourceRunner(ctx context.Context, identity *jobpb.NodeIdentity) error {
	_, err := c.connClient.RegisterSourceRunner(ctx, connect.NewRequest(identity))
	return err
}

func (c *JobConnectClient) DeregisterSourceRunner(ctx context.Context, op *jobpb.NodeIdentity) error {
	_, err := c.connClient.DeregisterSourceRunner(ctx, connect.NewRequest(op))
	return err
}

func (c *JobConnectClient) OperatorCheckpointComplete(ctx context.Context, cp *snapshotpb.OperatorCheckpoint) error {
	_, err := c.connClient.OperatorCheckpointComplete(ctx, connect.NewRequest(cp))
	return err
}

func (c *JobConnectClient) OnSourceCheckpointComplete(ctx context.Context, ckpt *snapshotpb.SourceCheckpoint) error {
	_, err := c.connClient.SourceCheckpointComplete(ctx, connect.NewRequest(ckpt))
	return err
}
