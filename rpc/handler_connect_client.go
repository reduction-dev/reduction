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
	connectClient   handlerpbconnect.HandlerClient
	keyEventBatcher *batching.EventBatcher[[]byte]
	keyEventResults chan []*handlerpb.KeyedEvent
}

type NewHandlerConnectClientParams struct {
	Host            string
	Opts            []connect.ClientOption
	BatchingOptions batching.EventBatcherParams
	ErrChan         chan<- error
}

func NewHandlerConnectClient(params NewHandlerConnectClientParams) *HandlerConnectClient {
	connectClient := handlerpbconnect.NewHandlerClient(NewHTTPClient(), "http://"+params.Host, params.Opts...)
	client := &HandlerConnectClient{
		connectClient: connectClient,
		keyEventBatcher: batching.NewEventBatcher[[]byte](context.Background(), batching.EventBatcherParams{
			MaxDelay: params.BatchingOptions.MaxDelay,
			MaxSize:  params.BatchingOptions.MaxSize,
		}),
		keyEventResults: make(chan []*handlerpb.KeyedEvent),
	}

	client.keyEventBatcher.OnBatchReady(func(batch [][]byte) {
		req := &handlerpb.KeyEventBatchRequest{
			Values: batch,
		}
		resp, err := client.connectClient.KeyEventBatch(context.Background(), connect.NewRequest(req))
		if err != nil {
			params.ErrChan <- err
			return
		}
		for _, result := range resp.Msg.Results {
			client.keyEventResults <- result.Events
		}
	})

	return client
}

func (h *HandlerConnectClient) KeyEvent(ctx context.Context, event []byte) {
	h.keyEventBatcher.Add(event)
}

func (h *HandlerConnectClient) OnEvent(ctx context.Context, req *handlerpb.OnEventRequest) (*handlerpb.HandlerResponse, error) {
	resp, err := h.connectClient.OnEvent(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}

	return resp.Msg, nil
}

func (h *HandlerConnectClient) OnTimerExpired(ctx context.Context, req *handlerpb.OnTimerExpiredRequest) (*handlerpb.HandlerResponse, error) {
	resp, err := h.connectClient.OnTimerExpired(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}

	return resp.Msg, nil
}

func (h *HandlerConnectClient) KeyEventResults() <-chan []*handlerpb.KeyedEvent {
	return h.keyEventResults
}

var _ proto.Handler = (*HandlerConnectClient)(nil)
