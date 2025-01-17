package e2e

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"reduction.dev/reduction/clocks"
	cfg "reduction.dev/reduction/config"
	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/connectors/httpapi"
	"reduction.dev/reduction/connectors/httpapi/httpapitest"
	"reduction.dev/reduction/jobs/jobstest"
	"reduction.dev/reduction/storage/localfs"
	"reduction.dev/reduction/workers/workerstest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type Event struct {
	UserID    string
	Timestamp time.Time
}

func TestCountInWindow(t *testing.T) {
	t.Parallel()

	httpAPIServer := httpapitest.StartServer()
	defer httpAPIServer.Close()
	httpAPIServer.WriteJSON("user-events", []any{
		Event{"user-1", time.UnixMilli(1)},   // timer: 2
		Event{"user-1", time.UnixMilli(2)},   // timer: 4
		Event{"user-1", time.UnixMilli(3)},   // timer: 4
		Event{"user-1", time.UnixMilli(4)},   // timer: 6
		Event{"user-1", time.UnixMilli(5)},   // timer: 6
		Event{"user-1", time.UnixMilli(6)},   // timer: 8
		Event{"user-1", time.UnixMilli(100)}, // High event to close the last window
	})

	jobConfig := &cfg.Config{
		WorkerCount: 1,
		Sources: []connectors.SourceConfig{httpapi.SourceConfig{
			Addr:   httpAPIServer.URL(),
			Topics: []string{"user-events"},
		}},
		Sinks: []connectors.SinkConfig{httpapi.SinkConfig{
			Addr: httpAPIServer.URL(),
		}},
		WorkingStorageLocation: t.TempDir(),
	}
	job, stop := jobstest.Run(jobConfig)
	defer stop()

	handler := NewCountInWindowHandler()
	handlerServer, stop := RunHandler(handler)
	defer stop()

	_, stop = workerstest.Run(t, workerstest.NewServerParams{
		HandlerAddr: handlerServer.Addr(),
		JobAddr:     job.RPCAddr(),
	})
	defer stop()

	require.EventuallyWithT(t, func(t *assert.CollectT) {
		assert.Equal(t, 4, httpAPIServer.RecordCount("egress-events"))
	}, time.Second*1, time.Millisecond*100)

	readResp := httpAPIServer.Read("egress-events")
	records := make([]CountInWindowEgressEvent, len(readResp.Events))
	for i, r := range readResp.Events {
		err := json.Unmarshal(r, &records[i])
		require.NoError(t, err)
	}

	assert.Equal(t, []CountInWindowEgressEvent{{
		UserID: "user-1",
		Count:  2,
	}, {
		UserID: "user-1",
		Count:  2,
	}, {
		UserID: "user-1",
		Count:  2,
	}, {
		UserID: "user-1",
		Count:  0,
	}}, records)
}

func TestCountInWindowRecoveryWithTimers(t *testing.T) {
	t.Parallel()

	httpAPIServer := httpapitest.StartServer(httpapitest.WithUnboundedReading())
	defer httpAPIServer.Close()
	httpAPIServer.WriteJSON("user-events", []any{
		Event{"user-1", time.UnixMilli(1)}, // timer: 2
		Event{"user-1", time.UnixMilli(2)}, // timer: 4
		Event{"user-1", time.UnixMilli(3)}, // timer: 4
	})

	testDir := t.TempDir()
	jobConfig := &cfg.Config{
		WorkerCount: 1,
		Sources: []connectors.SourceConfig{httpapi.SourceConfig{
			Addr:   httpAPIServer.URL(),
			Topics: []string{"user-events"},
		}},
		Sinks: []connectors.SinkConfig{httpapi.SinkConfig{
			Addr: httpAPIServer.URL(),
		}},
		WorkingStorageLocation: testDir,
	}
	clock := clocks.NewFrozenClock()
	jobStore := localfs.NewDirectory(filepath.Join(testDir, "job"))
	job, stop := jobstest.Run(jobConfig, jobstest.WithClock(clock), jobstest.WithStore(jobStore))
	defer stop()

	handler := NewCountInWindowHandler()
	handlerServer, stop := RunHandler(handler)
	defer stop()

	worker, stop := workerstest.Run(t, workerstest.NewServerParams{
		HandlerAddr: handlerServer.Addr(),
		JobAddr:     job.RPCAddr(),
	})
	defer stop()

	// Expect 1 egress event for the first window closing
	require.EventuallyWithT(t, func(t *assert.CollectT) {
		assert.Equal(t, 1, httpAPIServer.RecordCount("egress-events"))
	}, time.Second*1, time.Millisecond*100)

	// Checkpoint
	fsEvents := jobStore.Subscribe()
	clock.TickEvery("checkpointing")
	assert.Contains(t, <-fsEvents, ".snapshot")

	worker.Stop()
	job.Stop()

	// We must persist that timer 4 was set or we'll miss that window, resulting
	// in only 3 windows where the timer 6 window has 4 events instead of 2.
	httpAPIServer.WriteJSON("user-events", []any{
		Event{"user-1", time.UnixMilli(4)},   // timer: 6
		Event{"user-1", time.UnixMilli(5)},   // timer: 6
		Event{"user-1", time.UnixMilli(6)},   // timer: 8
		Event{"user-1", time.UnixMilli(100)}, // High event to close the last window
	})

	job, stop = jobstest.Run(jobConfig, jobstest.WithStore(jobStore))
	defer stop()

	_, stop = workerstest.Run(t, workerstest.NewServerParams{
		HandlerAddr: handlerServer.Addr(),
		JobAddr:     job.RPCAddr(),
	})
	defer stop()

	require.EventuallyWithT(t, func(t *assert.CollectT) {
		assert.Equal(t, 4, httpAPIServer.RecordCount("egress-events"))
	}, time.Second*1, time.Millisecond*100)

	readResp := httpAPIServer.Read("egress-events")
	records := make([]CountInWindowEgressEvent, len(readResp.Events))
	for i, r := range readResp.Events {
		err := json.Unmarshal(r, &records[i])
		require.NoError(t, err)
	}

	assert.Equal(t, []CountInWindowEgressEvent{{
		UserID: "user-1",
		Count:  2,
	}, {
		UserID: "user-1",
		Count:  2,
	}, {
		UserID: "user-1",
		Count:  2,
	}, {
		UserID: "user-1",
		Count:  0,
	}}, records)
}
