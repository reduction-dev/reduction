package rpc

import (
	"context"

	"reduction.dev/reduction-handler/handlerpb"
	"reduction.dev/reduction-handler/handlerpb/handlerpbconnect"
	"reduction.dev/reduction/proto"

	"connectrpc.com/connect"
)

type HandlerConnectClient struct {
	connClient handlerpbconnect.HandlerClient
}

func NewHandlerConnectClient(host string, opts ...connect.ClientOption) *HandlerConnectClient {
	client := handlerpbconnect.NewHandlerClient(NewHTTPClient(), "http://"+host, opts...)
	return &HandlerConnectClient{
		connClient: client,
	}
}

func (h *HandlerConnectClient) KeyEvent(ctx context.Context, req *handlerpb.KeyEventBatchRequest) (*handlerpb.KeyEventBatchResponse, error) {
	resp, err := h.connClient.KeyEventBatch(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}

	return resp.Msg, nil
}

func (h *HandlerConnectClient) OnEvent(ctx context.Context, req *handlerpb.OnEventRequest) (*handlerpb.HandlerResponse, error) {
	resp, err := h.connClient.OnEvent(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}

	return resp.Msg, nil
}

func (h *HandlerConnectClient) OnTimerExpired(ctx context.Context, req *handlerpb.OnTimerExpiredRequest) (*handlerpb.HandlerResponse, error) {
	resp, err := h.connClient.OnTimerExpired(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}

	return resp.Msg, nil
}

var _ proto.Handler = (*HandlerConnectClient)(nil)
