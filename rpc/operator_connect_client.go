package rpc

import (
	"context"

	"reduction.dev/reduction/proto"
	"reduction.dev/reduction/proto/jobpb"
	"reduction.dev/reduction/proto/workerpb"
	workerpbconnect "reduction.dev/reduction/proto/workerpb/workerpbconnect"

	"connectrpc.com/connect"
)

type OperatorConnectClient struct {
	id             string
	host           string
	operatorClient workerpbconnect.OperatorClient
}

func NewOperatorConnectClient(params *jobpb.NodeIdentity, opts ...connect.ClientOption) *OperatorConnectClient {
	if params.Host == "" {
		panic("missing host")
	}
	operatorClient := workerpbconnect.NewOperatorClient(NewHTTPClient(), "http://"+params.Host, opts...)
	return &OperatorConnectClient{
		id:             params.Id,
		host:           params.Host,
		operatorClient: operatorClient,
	}
}

func (c *OperatorConnectClient) HandleEvent(ctx context.Context, req *workerpb.HandleEventRequest) error {
	_, err := c.operatorClient.HandleEvent(ctx, connect.NewRequest(req))
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
