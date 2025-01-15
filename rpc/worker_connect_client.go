package rpc

import (
	"context"

	"reduction.dev/reduction/proto"
	"reduction.dev/reduction/proto/jobpb"
	"reduction.dev/reduction/proto/workerpb"
	workerpbconnect "reduction.dev/reduction/proto/workerpb/workerpbconnect"

	"connectrpc.com/connect"
)

type WorkerConnectClient struct {
	id             string
	host           string
	operatorClient workerpbconnect.OperatorClient
}

func NewWorkerOperatorClient(params *jobpb.NodeIdentity, opts ...connect.ClientOption) *WorkerConnectClient {
	if params.Host == "" {
		panic("missing host")
	}
	operatorClient := workerpbconnect.NewOperatorClient(NewHTTPClient(), "http://"+params.Host, opts...)
	return &WorkerConnectClient{
		id:             params.Id,
		host:           params.Host,
		operatorClient: operatorClient,
	}
}

func (c *WorkerConnectClient) HandleEvent(ctx context.Context, req *workerpb.HandleEventRequest) error {
	_, err := c.operatorClient.HandleEvent(ctx, connect.NewRequest(req))
	return err
}

func (c *WorkerConnectClient) Start(ctx context.Context, req *workerpb.StartOperatorRequest) error {
	_, err := c.operatorClient.Start(ctx, connect.NewRequest(req))
	return err
}

func (c *WorkerConnectClient) ID() string {
	return c.id
}

func (c *WorkerConnectClient) Host() string {
	return c.host
}

var _ proto.Operator = (*WorkerConnectClient)(nil)
