package rpc

import (
	"context"

	"reduction.dev/reduction/proto"
	"reduction.dev/reduction/proto/jobpb"
	"reduction.dev/reduction/proto/workerpb"
	workerpbconnect "reduction.dev/reduction/proto/workerpb/workerpbconnect"
	"reduction.dev/reduction/rpc/batching"

	"connectrpc.com/connect"
)

type OperatorConnectClient struct {
	id             string
	host           string
	senderID       string
	operatorClient workerpbconnect.OperatorClient
	eventBatcher   *batching.EventBatcher
}

type NewOperatorConnectClientParams struct {
	SenderID        string
	OperatorNode    *jobpb.NodeIdentity
	ConnectOptions  []connect.ClientOption
	BatchingOptions batching.EventBatcherParams
}

func NewOperatorConnectClient(params NewOperatorConnectClientParams) *OperatorConnectClient {
	if params.OperatorNode.Host == "" {
		panic("missing host")
	}
	operatorClient := workerpbconnect.NewOperatorClient(NewHTTPClient(), "http://"+params.OperatorNode.Host, params.ConnectOptions...)
	return &OperatorConnectClient{
		id:             params.OperatorNode.Id,
		host:           params.OperatorNode.Host,
		senderID:       params.SenderID,
		operatorClient: operatorClient,
		eventBatcher:   batching.NewEventBatcher(params.BatchingOptions),
	}
}

func (c *OperatorConnectClient) HandleEvent(ctx context.Context, event *workerpb.Event) error {
	req := &workerpb.HandleEventBatchRequest{
		SenderId: c.senderID,
		Events:   []*workerpb.Event{event}, // Starting by just sending one event in batch
	}
	_, err := c.operatorClient.HandleEventBatch(ctx, connect.NewRequest(req))
	return err
}

func (c *OperatorConnectClient) Start(ctx context.Context, req *workerpb.StartOperatorRequest) error {
	_, err := c.operatorClient.Start(ctx, connect.NewRequest(req))
	return err
}

func (c *OperatorConnectClient) ID() string {
	return c.id
}

func (c *OperatorConnectClient) Host() string {
	return c.host
}

var _ proto.Operator = (*OperatorConnectClient)(nil)

type OperatorClientFactory func(node *jobpb.NodeIdentity) *OperatorConnectClient
