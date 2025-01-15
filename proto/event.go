package proto

import (
	"fmt"

	"reduction.dev/reduction-handler/handlerpb"
	"reduction.dev/reduction/proto/workerpb"

	"google.golang.org/protobuf/proto"
)

// Given an event type, return the oneof wrapper type.
func PutOneOfEvent(event proto.Message) (*workerpb.Event, error) {
	req := &workerpb.Event{}
	switch typedEvent := event.(type) {
	case *handlerpb.KeyedEvent:
		req.Event = &workerpb.Event_KeyedEvent{KeyedEvent: typedEvent}
	case *workerpb.Watermark:
		req.Event = &workerpb.Event_Watermark{Watermark: typedEvent}
	case *workerpb.CheckpointBarrier:
		req.Event = &workerpb.Event_CheckpointBarrier{CheckpointBarrier: typedEvent}
	case *workerpb.SourceCompleteEvent:
		req.Event = &workerpb.Event_SourceComplete{SourceComplete: typedEvent}
	default:
		return nil, fmt.Errorf("unknown event type to send %v", typedEvent)
	}

	return req, nil
}
