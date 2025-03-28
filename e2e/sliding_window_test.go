package e2e

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"reduction.dev/reduction-go/connectors/httpapi"
	"reduction.dev/reduction-go/rxn"
	"reduction.dev/reduction-go/topology"
	"reduction.dev/reduction/connectors/httpapi/httpapitest"
	"reduction.dev/reduction/jobs/jobstest"
	"reduction.dev/reduction/workers/workerstest"
)

// This test demonstrates a use case for using the current watermark in a
// handler. The handler sets a new timer in the `OnTimerExpired` method based on
// the current watermark to ensure that a timer triggers at the next watermark
// is not lost.
func TestSlidingWindow(t *testing.T) {
	t.Parallel()

	httpAPIServer := httpapitest.StartServer(httpapitest.WithUnboundedReading())
	defer httpAPIServer.Close()

	jobDef := &topology.Job{
		WorkerCount:            topology.IntValue(1),
		WorkingStorageLocation: topology.StringValue(t.TempDir()),
	}

	source := httpapi.NewSource(jobDef, "Source", &httpapi.SourceParams{
		Addr:     topology.StringValue(httpAPIServer.URL()),
		Topics:   []string{"events"},
		KeyEvent: KeySlidingWindowEvent,
	})
	sink := httpapi.NewSink(jobDef, "Sink", &httpapi.SinkParams{
		Addr: topology.StringValue(httpAPIServer.URL()),
	})
	operator := topology.NewOperator(jobDef, "Operator", &topology.OperatorParams{
		Handler: func(op *topology.Operator) rxn.OperatorHandler {
			return NewSlidingWindowHandler(sink, op)
		},
	})
	source.Connect(operator)
	operator.Connect(sink)

	job, stop := jobstest.Run(jobDef)
	defer stop()

	handlerServer, stop := RunHandler(jobDef)
	defer stop()

	_, stop = workerstest.Run(t, workerstest.NewServerParams{
		HandlerAddr: handlerServer.Addr(),
		JobAddr:     job.RPCAddr(),
	})
	defer stop()

	// Add two events in the first minute and two in the the second minute
	baseTime := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC)
	httpAPIServer.WriteJSON("events", []any{
		SlidingWindowEvent{Key: "user1", Timestamp: baseTime},
		SlidingWindowEvent{Key: "user1", Timestamp: baseTime.Add(1 * time.Second)},
		SlidingWindowEvent{Key: "user1", Timestamp: baseTime.Add(1 * time.Minute)},
		SlidingWindowEvent{Key: "user1", Timestamp: baseTime.Add(1 * time.Minute).Add(1 * time.Second)},
	})

	// The first sliding window from user1 should be closed
	require.EventuallyWithT(t, func(t *assert.CollectT) {
		require.Equal(t, 1, httpAPIServer.RecordCount("windows"))
	}, time.Second, 10*time.Millisecond)

	// A second user advances the event more than the sliding interval of 1m
	httpAPIServer.WriteJSON("events", []any{
		SlidingWindowEvent{Key: "user2", Timestamp: baseTime.Add(30 * time.Minute)},
	})

	// The second sliding window from user1 closes after user2 advances the event time
	require.EventuallyWithT(t, func(t *assert.CollectT) {
		require.Equal(t, 2, httpAPIServer.RecordCount("windows"))
	}, time.Second, 10*time.Millisecond)

	// User 2 advances time again by 30m
	httpAPIServer.WriteJSON("events", []any{
		SlidingWindowEvent{Key: "user2", Timestamp: baseTime.Add(60 * time.Minute)},
	})

	// The first sliding window from user2 closes, also triggering another user1 window
	require.EventuallyWithT(t, func(t *assert.CollectT) {
		require.Equal(t, 4, httpAPIServer.RecordCount("windows"))
	}, time.Second, 10*time.Millisecond)

	windows := make([]SlidingWindowOutput, httpAPIServer.RecordCount("windows"))
	for i, event := range httpAPIServer.Read("windows").Events {
		var w SlidingWindowOutput
		require.NoError(t, json.Unmarshal(event, &w))
		windows[i] = w
	}

	// Filter to only show user1 windows
	var user1Windows []SlidingWindowOutput
	for _, window := range windows {
		if window.Key == "user1" {
			user1Windows = append(user1Windows, window)
		}
	}

	assert.Equal(t, []SlidingWindowOutput{
		// First two windows for user1
		{Interval: "2025-01-01T09:01:00Z/2025-01-01T10:01:00Z", Sum: 2, Key: "user1"},
		{Interval: "2025-01-01T09:02:00Z/2025-01-01T10:02:00Z", Sum: 4, Key: "user1"},

		// A window for user1 due to user2 advancing time
		{Interval: "2025-01-01T09:30:00Z/2025-01-01T10:30:00Z", Sum: 4, Key: "user1"}, // This window is missing
	}, user1Windows)
}
