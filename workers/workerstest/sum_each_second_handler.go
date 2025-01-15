package workerstest

import (
	"context"
	"encoding/json"
	"slices"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	"reduction.dev/reduction-handler/handlerpb"
	"reduction.dev/reduction/proto"
	"reduction.dev/reduction/util/sliceu"
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

func (s *SumEachSecondHandler) KeyEvent(ctx context.Context, req *handlerpb.KeyEventRequest) (*handlerpb.KeyEventResponse, error) {
	panic("unused for test")
}

func (s *SumEachSecondHandler) OnEvent(ctx context.Context, req *handlerpb.OnEventRequest) (*handlerpb.HandlerResponse, error) {
	// Decode the user event
	var event SumEvent
	err := json.Unmarshal(req.Event.Value, &event)
	if err != nil {
		return nil, err
	}

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

	// Create a timer for the next second
	timer := event.Timestamp.Add(time.Second - 1).Truncate(time.Second)

	return &handlerpb.HandlerResponse{
		NewTimers: []*timestamppb.Timestamp{timestamppb.New(timer)},
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
	}, nil
}

func (s *SumEachSecondHandler) OnTimerExpired(ctx context.Context, req *handlerpb.OnTimerExpiredRequest) (*handlerpb.HandlerResponse, error) {
	// Find the entries in "sum" namespace.
	var sum *handlerpb.StateEntryNamespace
	for _, ns := range req.StateEntryNamespaces {
		if ns.Namespace == "sum" {
			sum = ns
			break
		}
	}

	sinkRequests := sliceu.Map(sum.GetEntries(), func(entry *handlerpb.StateEntry) *handlerpb.SinkRequest {
		return &handlerpb.SinkRequest{Id: "sink", Value: entry.Value}
	})
	return &handlerpb.HandlerResponse{
		SinkRequests: sinkRequests,
	}, nil
}

var _ proto.Handler = (*SumEachSecondHandler)(nil)
