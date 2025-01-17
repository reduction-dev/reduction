package rpc

import (
	"context"
	"errors"

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
	cancelFunc    context.CancelCauseFunc
}

type NewOperatorConnectClientParams struct {
	SenderID        string
	OperatorNode    *jobpb.NodeIdentity
	ConnectOptions  []connect.ClientOption
	BatchingOptions batching.EventBatcherParams
	ErrChan         chan<- error
}

func NewOperatorConnectClient(params NewOperatorConnectClientParams) (client *OperatorConnectClient) {
	if params.OperatorNode.Host == "" {
		panic("missing host")
	}
	connectClient := workerpbconnect.NewOperatorClient(NewHTTPClient(), "http://"+params.OperatorNode.Host, params.ConnectOptions...)
	client = &OperatorConnectClient{
		id:            params.OperatorNode.Id,
		host:          params.OperatorNode.Host,
		senderID:      params.SenderID,
		connectClient: connectClient,
		eventBatcher:  batching.NewEventBatcher(params.BatchingOptions),
	}

	ctx, cancelFunc := context.WithCancelCause(context.Background())
	client.cancelFunc = cancelFunc

	client.eventBatcher.OnBatchReady(func(batch []*workerpb.Event) {
		req := &workerpb.HandleEventBatchRequest{
			SenderId: client.senderID,
			Events:   batch,
		}
		_, err := client.connectClient.HandleEventBatch(ctx, connect.NewRequest(req))
		if params.ErrChan != nil && err != nil && !errors.Is(err, context.Canceled) {
			params.ErrChan <- err
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

func (c *OperatorConnectClient) Close() {
	c.cancelFunc(ErrClientClosed)
	c.eventBatcher.Close()
}

var _ proto.Operator = (*OperatorConnectClient)(nil)

var ErrClientClosed = errors.New("client closed")
