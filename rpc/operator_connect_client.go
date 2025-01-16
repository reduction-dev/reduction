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
	id            string
	host          string
	senderID      string
	connectClient workerpbconnect.OperatorClient
	eventBatcher  *batching.EventBatcher
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
	connectClient := workerpbconnect.NewOperatorClient(NewHTTPClient(), "http://"+params.OperatorNode.Host, params.ConnectOptions...)
	client := &OperatorConnectClient{
		id:            params.OperatorNode.Id,
		host:          params.OperatorNode.Host,
		senderID:      params.SenderID,
		connectClient: connectClient,
		eventBatcher:  batching.NewEventBatcher(params.BatchingOptions),
	}
	client.eventBatcher.OnBatchReady(func(batch []*workerpb.Event) {
		req := &workerpb.HandleEventBatchRequest{
			SenderId: client.senderID,
			Events:   batch,
		}
		_, err := client.connectClient.HandleEventBatch(context.Background(), connect.NewRequest(req))
		if err != nil {
			panic(err)
		}
	})

	return client
}

func (c *OperatorConnectClient) HandleEvent(ctx context.Context, event *workerpb.Event) error {
	c.eventBatcher.Add(event)
	return nil
}

func (c *OperatorConnectClient) Start(ctx context.Context, req *workerpb.StartOperatorRequest) error {
	_, err := c.connectClient.Start(ctx, connect.NewRequest(req))
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
