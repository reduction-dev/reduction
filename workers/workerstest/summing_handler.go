package workerstest

import (
	"context"
	"encoding/json"
	"slices"

	"reduction.dev/reduction-handler/handlerpb"
	"reduction.dev/reduction/proto"
)

type SummingHandler struct{}

func (s *SummingHandler) KeyEvent(ctx context.Context, req *handlerpb.KeyEventRequest) (*handlerpb.KeyEventResponse, error) {
	panic("unused by operators")
}

func (s *SummingHandler) OnEvent(ctx context.Context, req *handlerpb.OnEventRequest) (*handlerpb.HandlerResponse, error) {
	// Find and decode a state entry for "sum" if available
	var sumState SumState
	snIndex := slices.IndexFunc(req.StateEntryNamespaces, func(ns *handlerpb.StateEntryNamespace) bool {
		return ns.Namespace == "sum"
	})
	if snIndex > -1 { // found
		entries := req.StateEntryNamespaces[snIndex].Entries
		if len(entries) > 0 {
			if err := json.Unmarshal(entries[0].Value, &sumState); err != nil {
				return nil, err
			}
		}
	}

	// Add one and encode state item
	sumState.Sum = sumState.Sum + 1
	nextSumState, err := json.Marshal(sumState)
	if err != nil {
		return nil, err
	}

	return &handlerpb.HandlerResponse{
		StateMutationNamespaces: []*handlerpb.StateMutationNamespace{{
			Namespace: "sum",
			Mutations: []*handlerpb.StateMutation{{
				Mutation: &handlerpb.StateMutation_Put{
					Put: &handlerpb.PutMutation{
						Key:   []byte("sum"),
						Value: nextSumState,
					},
				},
			}},
		}},
		SinkRequests: []*handlerpb.SinkRequest{{
			Id:    "sink",
			Value: nextSumState,
		}},
	}, nil
}

func (s *SummingHandler) OnTimerExpired(ctx context.Context, req *handlerpb.OnTimerExpiredRequest) (*handlerpb.HandlerResponse, error) {
	panic("unused, handler sets no timers")
}

var _ proto.Handler = (*SummingHandler)(nil)
