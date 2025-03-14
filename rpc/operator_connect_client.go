package rpc

import (
	"context"
	"log/slog"

	"reduction.dev/reduction/proto"
	"reduction.dev/reduction/proto/jobpb"
	"reduction.dev/reduction/proto/workerpb"
	workerpbconnect "reduction.dev/reduction/proto/workerpb/workerpbconnect"

	"connectrpc.com/connect"
)

type OperatorConnectClient struct {
	id            string
	host          string
	senderID      string
	connectClient workerpbconnect.OperatorClient
}

type NewOperatorConnectClientParams struct {
	SenderID       string
	OperatorNode   *jobpb.NodeIdentity
	ConnectOptions []connect.ClientOption
}

func NewOperatorConnectClient(params NewOperatorConnectClientParams) (client *OperatorConnectClient) {
	if params.OperatorNode.Host == "" {
		panic("missing host")
	}

	return &OperatorConnectClient{
		id:       params.OperatorNode.Id,
		host:     params.OperatorNode.Host,
		senderID: params.SenderID,
		connectClient: workerpbconnect.NewOperatorClient(
			NewHTTPClient("operator", slog.Default()),
			"http://"+params.OperatorNode.Host, params.ConnectOptions...),
	}
}

func (c *OperatorConnectClient) ID() string {
	return c.id
}

func (c *OperatorConnectClient) Host() string {
	return c.host
}

func (c *OperatorConnectClient) HandleEventBatch(ctx context.Context, events []*workerpb.Event) error {
	_, err := c.connectClient.HandleEventBatch(ctx, connect.NewRequest(&workerpb.HandleEventBatchRequest{
		SenderId: c.senderID,
		Events:   events,
	}))
	return err
}

func (c *OperatorConnectClient) Start(ctx context.Context, req *workerpb.StartOperatorRequest) error {
	_, err := c.connectClient.Start(ctx, connect.NewRequest(req))
	return err
}

func (c *OperatorConnectClient) UpdateRetainedCheckpoints(ctx context.Context, ids []uint64) error {
	_, err := c.connectClient.UpdateRetainedCheckpoints(ctx, connect.NewRequest(&workerpb.UpdateRetainedCheckpointsRequest{
		CheckpointIds: ids,
	}))
	return err
}

var _ proto.Operator = (*OperatorConnectClient)(nil)
