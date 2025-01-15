package operator_test

import (
	"context"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/types/known/timestamppb"
	"reduction.dev/reduction-handler/handlerpb"
	"reduction.dev/reduction/connectors/embedded"
	"reduction.dev/reduction/proto/snapshotpb"
	"reduction.dev/reduction/proto/workerpb"
	"reduction.dev/reduction/util/sliceu"
	"reduction.dev/reduction/workers/operator"
	"reduction.dev/reduction/workers/workerstest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test that a single operator with two upstream sources only fires OnEvent
// for timers based on the minimum watermark provided by the sources.
// The requires that the operator understand its upstream sources.
func TestOperatorUsesMinimumOfSourceWatermarks(t *testing.T) {
	g := errgroup.Group{}
	op := operator.NewOperator(operator.NewOperatorParams{
		ID:          "op1",
		UserHandler: &workerstest.SumEachSecondHandler{},
		Job:         &workerstest.DummyJob{},
	})
	g.Go(func() error {
		return op.Start(context.Background())
	})
	ctx := context.Background()

	sink := &embedded.RecordingSink{}
	op.HandleStart(ctx, &workerpb.StartOperatorRequest{
		OperatorIds:     []string{"op1"},
		SourceRunnerIds: []string{"sr1", "sr2"},
		KeyGroupCount:   256,
		StorageLocation: "memory:///storage",
	}, sink)

	// Sum to 2
	err := op.HandleEvent(ctx, "sr1", &workerpb.Event{
		Event: &workerpb.Event_KeyedEvent{
			KeyedEvent: &handlerpb.KeyedEvent{
				Key:   []byte("static"),
				Value: workerstest.SumEvent{Timestamp: time.UnixMilli(1)}.Marshal(),
			},
		},
	})
	require.NoError(t, err)
	err = op.HandleEvent(context.Background(), "sr1", &workerpb.Event{
		Event: &workerpb.Event_KeyedEvent{
			KeyedEvent: &handlerpb.KeyedEvent{
				Key:   []byte("static"),
				Value: workerstest.SumEvent{Timestamp: time.UnixMilli(2)}.Marshal(),
			},
		},
	})
	require.NoError(t, err)

	// Watermark to 1s on single task doesn't fire timer
	op.HandleEvent(context.Background(), "sr1", &workerpb.Event{
		Event: &workerpb.Event_Watermark{
			Watermark: &workerpb.Watermark{
				Timestamp: &timestamppb.Timestamp{Seconds: 1},
			},
		},
	})
	assert.Len(t, sink.Values, 0)

	// After second task passes watermark, timers can run
	op.HandleEvent(context.Background(), "sr2", &workerpb.Event{
		Event: &workerpb.Event_Watermark{
			Watermark: &workerpb.Watermark{
				Timestamp: &timestamppb.Timestamp{Seconds: 1},
			},
		},
	})
	assert.Len(t, sink.Values, 1)
	assert.Equal(t, 2, workerstest.UnmarshalSumState(sink.Values[0]).Sum)

	require.NoError(t, op.Stop())
	require.NoError(t, g.Wait())
}

// Test that a single operator with two upstream sources pauses reading from
// sources until all checkpoint barriers have arrived. This is required to make
// sure that the checkpoint _only_ contains state from events that are in front
// of all the checkpoint barriers of an aggregate checkpoint.
func TestOperatorAlignsOnCheckpointBarriers(t *testing.T) {
	job := &workerstest.DummyJob{}
	op := operator.NewOperator(operator.NewOperatorParams{
		ID:          "op1",
		UserHandler: &workerstest.SummingHandler{},
		Job:         job,
	})
	go func() {
		op.Start(context.Background())
	}()
	defer op.Stop()

	tmpDir := t.TempDir()
	sink := &embedded.RecordingSink{}
	op.HandleStart(context.Background(), &workerpb.StartOperatorRequest{
		OperatorIds:     []string{"op1"},
		SourceRunnerIds: []string{"sr1", "sr2"},
		KeyGroupCount:   256,
		StorageLocation: tmpDir,
	}, sink)

	// Send 2 events
	err := op.HandleEvent(context.Background(), "sr1", &workerpb.Event{
		Event: &workerpb.Event_KeyedEvent{
			KeyedEvent: &handlerpb.KeyedEvent{
				Key: []byte("static"),
			},
		},
	})
	require.NoError(t, err)
	err = op.HandleEvent(context.Background(), "sr1", &workerpb.Event{
		Event: &workerpb.Event_KeyedEvent{
			KeyedEvent: &handlerpb.KeyedEvent{
				Key: []byte("static"),
			},
		},
	})
	require.NoError(t, err)

	sendEventsGroup := errgroup.Group{}
	sendEventsGroup.Go(func() error {
		// Send a checkpoint barrier from the first source (expected to block until 2nd barrier arrives)
		err := op.HandleEvent(context.Background(), "sr1", &workerpb.Event{
			Event: &workerpb.Event_CheckpointBarrier{
				CheckpointBarrier: &workerpb.CheckpointBarrier{},
			},
		})
		require.NoError(t, err)

		return op.HandleEvent(context.Background(), "sr1", &workerpb.Event{
			Event: &workerpb.Event_KeyedEvent{
				KeyedEvent: &handlerpb.KeyedEvent{
					Key: []byte("static"),
				},
			},
		})
	})

	// Send a checkpoint barrier from the second source
	err = op.HandleEvent(context.Background(), "sr2", &workerpb.Event{
		Event: &workerpb.Event_CheckpointBarrier{
			CheckpointBarrier: &workerpb.CheckpointBarrier{},
		},
	})
	require.NoError(t, err)

	require.NoError(t, sendEventsGroup.Wait())

	// Wait for operator to complete checkpoint with job
	require.Eventually(t, func() bool {
		return job.OperatorCheckpoint != nil
	}, 1*time.Second, 100*time.Millisecond)

	// All events have been processed
	assert.Len(t, sink.Values, 3)
	assert.Equal(t, 3, workerstest.UnmarshalSumState(sliceu.Last(sink.Values)).Sum)

	// Restart the operator from the checkpoint
	op.HandleStart(context.Background(), &workerpb.StartOperatorRequest{
		OperatorIds:     []string{"op1"},
		SourceRunnerIds: []string{"sr1", "sr2"},
		KeyGroupCount:   256,
		StorageLocation: tmpDir,
		Checkpoints:     []*snapshotpb.OperatorCheckpoint{job.OperatorCheckpoint},
	}, sink)

	// Send another event from sr1 to trigger new sum sink event
	err = op.HandleEvent(context.Background(), "sr1", &workerpb.Event{
		Event: &workerpb.Event_KeyedEvent{
			KeyedEvent: &handlerpb.KeyedEvent{
				Key: []byte("static"),
			},
		},
	})
	require.NoError(t, err)

	// We end up with the initial 3 events and event after restart.
	assert.Len(t, sink.Values, 4)
	assert.Equal(t, 3, workerstest.UnmarshalSumState(sliceu.Last(sink.Values)).Sum)
}
