package workerstest

import (
	"context"
	"encoding/json"
	"slices"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	"reduction.dev/reduction-protocol/handlerpb"
	"reduction.dev/reduction/proto"
)

type SumEachSecondHandler struct{}

type SumEvent struct {
	Timestamp time.Time
}

func (se SumEvent) Marshal() []byte {
	b, err := json.Marshal(se)
	if err != nil {
		panic(err)
	}
	return b
}

type SumState struct {
	Sum int
}

func UnmarshalSumState(b []byte) SumState {
	ss := SumState{}
	if err := json.Unmarshal(b, &ss); err != nil {
		panic(err)
	}
	return ss
}

func (ss SumState) Marshal() []byte {
	b, err := json.Marshal(ss)
	if err != nil {
		panic(err)
	}
	return b
}

func (s *SumEachSecondHandler) KeyEventBatch(ctx context.Context, events [][]byte) ([][]*handlerpb.KeyedEvent, error) {
	panic("unused for test")
}

// KeyEventResults implements proto.Handler.
func (s *SumEachSecondHandler) KeyEventResults() <-chan []*handlerpb.KeyedEvent {
	panic("unused for test")
}

func (s *SumEachSecondHandler) ProcessEventBatch(ctx context.Context, req *handlerpb.ProcessEventBatchRequest) (*handlerpb.ProcessEventBatchResponse, error) {
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
		switch typedEvent := event.Event.(type) {

		// For every event, update the state and create a timer for the next second
		case *handlerpb.Event_KeyedEvent:
			// Decode the user sumEvent
			var sumEvent SumEvent
			err := json.Unmarshal(typedEvent.KeyedEvent.Value, &sumEvent)
			if err != nil {
				return nil, err
			}

			// Create a timer for the next second
			newTimer := sumEvent.Timestamp.Add(time.Second - 1).Truncate(time.Second)

			// Increment the sum
			sumState := state[string(typedEvent.KeyedEvent.Key)]
			sumState.Sum++
			state[string(typedEvent.KeyedEvent.Key)] = sumState

			// Add the mutation and timer for this key
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
				NewTimers: []*timestamppb.Timestamp{timestamppb.New(newTimer)},
			})

		case *handlerpb.Event_TimerExpired:
			sum := state[string(typedEvent.TimerExpired.Key)]
			sinkRequests = append(sinkRequests, &handlerpb.SinkRequest{
				Id:    "sink",
				Value: sum.Marshal(),
			})
		}
	}

	return &handlerpb.ProcessEventBatchResponse{
		SinkRequests: sinkRequests,
		KeyResults:   keyResults,
	}, nil
}

var _ proto.Handler = (*SumEachSecondHandler)(nil)
