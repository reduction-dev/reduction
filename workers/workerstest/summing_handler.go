package workerstest

import (
	"context"
	"encoding/json"
	"slices"

	"reduction.dev/reduction-handler/handlerpb"
	"reduction.dev/reduction/proto"
)

type SummingHandler struct{}

func (s *SummingHandler) KeyEvent(ctx context.Context, event []byte) {
	panic("unused by operators")
}

func (s *SummingHandler) KeyEventResults() <-chan []*handlerpb.KeyedEvent {
	panic("unused by operators")
}

func (s *SummingHandler) ProcessEventBatch(ctx context.Context, req *handlerpb.ProcessEventBatchRequest) (*handlerpb.ProcessEventBatchResponse, error) {
	// Use the req.KeyStates to construct a state store that will be mutated by the events
	state := make(map[string]SumState, len(req.KeyStates))
	for _, keyState := range req.KeyStates {
		// Find and decode a state entry for "sum" if available
		var sumState SumState
		snIndex := slices.IndexFunc(keyState.StateEntryNamespaces, func(ns *handlerpb.StateEntryNamespace) bool {
			return ns.Namespace == "sum"
		})
		if snIndex > -1 { // found
			entries := keyState.StateEntryNamespaces[snIndex].Entries
			if len(entries) > 0 {
				if err := json.Unmarshal(entries[0].Value, &sumState); err != nil {
					return nil, err
				}
			}
		}

		state[string(keyState.Key)] = sumState
	}

	var keyResults []*handlerpb.KeyResult
	var sinkRequests []*handlerpb.SinkRequest
	for _, event := range req.Events {
		if typedEvent, ok := event.Event.(*handlerpb.Event_KeyedEvent); ok {
			// Increment the sum
			sumState := state[string(typedEvent.KeyedEvent.Key)]
			sumState.Sum++
			state[string(typedEvent.KeyedEvent.Key)] = sumState

			// Add the mutation for this key
			nextSumState, err := json.Marshal(sumState)
			if err != nil {
				return nil, err
			}
			keyResults = append(keyResults, &handlerpb.KeyResult{
				Key: typedEvent.KeyedEvent.Key,
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
			})

			sinkRequests = append(sinkRequests, &handlerpb.SinkRequest{
				Id:    "sink",
				Value: nextSumState,
			})
		}
	}

	return &handlerpb.ProcessEventBatchResponse{
		SinkRequests: sinkRequests,
		KeyResults:   keyResults,
	}, nil
}

var _ proto.Handler = (*SummingHandler)(nil)
