package testrun

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"time"

	gproto "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"reduction.dev/reduction-protocol/jobconfigpb"
	"reduction.dev/reduction-protocol/testrunpb"
	"reduction.dev/reduction/proto"
	"reduction.dev/reduction/proto/workerpb"
	"reduction.dev/reduction/rpc"
	"reduction.dev/reduction/workers/operator"
)

func Run(input io.Reader, output io.Writer) error {
	// First phase: collect all events
	events, err := readEvents(input)
	if err != nil {
		return fmt.Errorf("failed to read events: %w", err)
	}

	// Second phase: process events through sourcerunner
	op := operator.NewOperator(operator.NewOperatorParams{
		ID:          "testrun",
		Host:        "testrun",
		Job:         &proto.NoopJob{},
		UserHandler: rpc.NewHandlerPipeClient(input, output),
	})

	opResult := make(chan error, 1)
	go func() {
		opResult <- op.Start(context.Background())
	}()

	err = op.HandleStart(context.Background(), &workerpb.StartOperatorRequest{
		OperatorIds:     []string{"testrun"},
		SourceRunnerIds: []string{"testrun"},
		KeyGroupCount:   1,
		Sinks:           []*jobconfigpb.Sink{},
		StorageLocation: "memory:///storage",
	}, nil)
	if err != nil {
		return fmt.Errorf("failed to start operator: %w", err)
	}

	for _, event := range events {
		if err = op.HandleEvent(context.Background(), "testrun", event); err != nil {
			return fmt.Errorf("failed to handle event: %w", err)
		}
	}

	if err := op.Stop(); err != nil {
		return fmt.Errorf("failed to stop operator: %w", err)
	}
	return <-opResult
}

// Read events from reader until reaching Run command
func readEvents(reader io.Reader) ([]*workerpb.Event, error) {
	var events []*workerpb.Event
	watermarkTime := time.Unix(0, 0)

	for {
		var length uint32
		if err := binary.Read(reader, binary.BigEndian, &length); err != nil {
			if err == io.EOF {
				return nil, fmt.Errorf("unexpected EOF before Run command")
			}
			return nil, fmt.Errorf("failed to read message length: %w", err)
		}

		data := make([]byte, length)
		if _, err := io.ReadFull(reader, data); err != nil {
			return nil, fmt.Errorf("failed to read message data: %w", err)
		}

		var cmd testrunpb.RunnerCommand
		if err := gproto.Unmarshal(data, &cmd); err != nil {
			return nil, fmt.Errorf("failed to unmarshal RunnerCommand: %w", err)
		}

		switch c := cmd.Command.(type) {
		case *testrunpb.RunnerCommand_AddKeyedEvent:
			events = append(events, &workerpb.Event{
				Event: &workerpb.Event_KeyedEvent{
					KeyedEvent: c.AddKeyedEvent.KeyedEvent,
				},
			})

			// Advance watermark time
			ts := c.AddKeyedEvent.KeyedEvent.Timestamp.AsTime()
			if ts.After(watermarkTime) {
				watermarkTime = ts
			}

		case *testrunpb.RunnerCommand_AddWatermark:
			events = append(events, &workerpb.Event{
				Event: &workerpb.Event_Watermark{
					Watermark: &workerpb.Watermark{
						Timestamp: timestamppb.New(watermarkTime),
					},
				},
			})

		case *testrunpb.RunnerCommand_Run:
			return events, nil
		}
	}
}
