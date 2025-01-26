package rpc

import (
	"context"

	"reduction.dev/reduction-handler/handlerpb"
	"reduction.dev/reduction-handler/handlerpb/handlerpbconnect"
	"reduction.dev/reduction/proto"
	"reduction.dev/reduction/rpc/batching"

	"connectrpc.com/connect"
)

type HandlerConnectClient struct {
	connectClient handlerpbconnect.HandlerClient
}

type NewHandlerConnectClientParams struct {
	Host            string
	Opts            []connect.ClientOption
	BatchingOptions batching.EventBatcherParams
	ErrChan         chan<- error
}

func NewHandlerConnectClient(params NewHandlerConnectClientParams) *HandlerConnectClient {
	connectClient := handlerpbconnect.NewHandlerClient(NewHTTPClient("handler"), "http://"+params.Host, params.Opts...)
	client := &HandlerConnectClient{
		connectClient: connectClient,
	}

	return client
}

func (h *HandlerConnectClient) ProcessEventBatch(ctx context.Context, req *handlerpb.ProcessEventBatchRequest) (*handlerpb.ProcessEventBatchResponse, error) {
	resp, err := h.connectClient.ProcessEventBatch(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}

	return resp.Msg, nil
}

func (h *HandlerConnectClient) KeyEventBatch(ctx context.Context, events [][]byte) ([][]*handlerpb.KeyedEvent, error) {
	req := connect.NewRequest(&handlerpb.KeyEventBatchRequest{Values: events})
	resp, err := h.connectClient.KeyEventBatch(ctx, req)
	if err != nil {
		return nil, err
	}

	keyedEvents := make([][]*handlerpb.KeyedEvent, len(resp.Msg.Results))
	for i, result := range resp.Msg.Results {
		keyedEvents[i] = result.Events
	}
	return keyedEvents, nil
}

var _ proto.Handler = (*HandlerConnectClient)(nil)
