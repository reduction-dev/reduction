package e2e_test

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"reduction.dev/reduction-go/connectors/httpapi"
	"reduction.dev/reduction-go/rxn"
	"reduction.dev/reduction-go/topology"
	"reduction.dev/reduction/clocks"
	"reduction.dev/reduction/connectors/httpapi/httpapitest"
	"reduction.dev/reduction/e2e"
	"reduction.dev/reduction/jobs/jobstest"
	"reduction.dev/reduction/storage/locations"
	"reduction.dev/reduction/util/binu"
	"reduction.dev/reduction/workers/workerstest"
)

// This test demonstrates that multiple checkpoints can proceed with 2 workers
func TestCheckpointAlignment(t *testing.T) {
	t.Parallel()

	// Start an HTTP source with test data
	httpAPIServer := httpapitest.StartServer(httpapitest.WithUnboundedReading())
	defer httpAPIServer.Close()
	httpAPIServer.WriteBatch("events", binu.IntBytesList(1, 2, 3))

	// Create job definition with 2 workers
	testDir := t.TempDir()
	jobDef := &topology.Job{
		WorkerCount:            topology.IntValue(2),
		WorkingStorageLocation: topology.StringValue(testDir),
	}

	// Set up source, sink and operator
	source := httpapi.NewSource(jobDef, "Source", &httpapi.SourceParams{
		Addr:     topology.StringValue(httpAPIServer.URL()),
		Topics:   []string{"events"},
		KeyEvent: e2e.KeyEventWithUniformKeyAndZeroTimestamp,
	})
	sink := httpapi.NewSink(jobDef, "Sink", &httpapi.SinkParams{
		Addr: topology.StringValue(httpAPIServer.URL()),
	})
	operator := topology.NewOperator(jobDef, "Operator", &topology.OperatorParams{
		Handler: func(op *topology.Operator) rxn.OperatorHandler {
			return e2e.NewPassThroughHandler(sink, "output")
		},
	})
	source.Connect(operator)
	operator.Connect(sink)

	// Start the job
	clock := clocks.NewFrozenClock()
	jobStore := locations.NewLocalDirectory(filepath.Join(testDir, "job"))
	job, stopJob := jobstest.Run(jobDef,
		jobstest.WithClock(clock),
		jobstest.WithStore(jobStore))
	defer stopJob()

	// Start the handler server
	handlerServer, stopHandler := e2e.RunHandler(jobDef)
	defer stopHandler()

	// Start two worker instances
	_, stopWorker1 := workerstest.Run(t, workerstest.NewServerParams{
		LogPrefix:   "worker-1",
		HandlerAddr: handlerServer.Addr(),
		JobAddr:     job.RPCAddr(),
		Clock:       clock,
	})
	defer stopWorker1()

	_, stopWorker2 := workerstest.Run(t, workerstest.NewServerParams{
		LogPrefix:   "worker-2",
		HandlerAddr: handlerServer.Addr(),
		JobAddr:     job.RPCAddr(),
		Clock:       clock,
	})
	defer stopWorker2()

	// Wait for data processing to complete
	require.EventuallyWithT(t, func(t *assert.CollectT) {
		assert.Equal(t, 3, httpAPIServer.RecordCount("output"))
	}, time.Second*1, time.Millisecond*100)

	// Trigger 2 checkpoints and verify they complete
	snapshotCreated := make(chan struct{}, 2)

	// Start goroutine to continuously read from fsEvents channel
	fsEvents := jobStore.Subscribe()
	go func() {
		for fileEvent := range fsEvents {
			if fileEvent.Op == locations.OpCreate && strings.Contains(fileEvent.Path, ".snapshot") {
				snapshotCreated <- struct{}{}
			}
		}
	}()

	// Process more data
	httpAPIServer.WriteBatch("events", binu.IntBytesList(1, 2, 3))
	require.EventuallyWithT(t, func(t *assert.CollectT) {
		assert.Equal(t, 6, httpAPIServer.RecordCount("output"))
	}, time.Second*1, time.Millisecond*100)

	// Trigger first checkpoint
	clock.TickEvery("checkpointing")
	<-snapshotCreated

	// Process more data
	httpAPIServer.WriteBatch("events", binu.IntBytesList(1, 2, 3))
	require.EventuallyWithT(t, func(t *assert.CollectT) {
		assert.Equal(t, 9, httpAPIServer.RecordCount("output"))
	}, time.Second*1, time.Millisecond*100)

	// Trigger second checkpoint
	clock.TickEvery("checkpointing")
	<-snapshotCreated
}
