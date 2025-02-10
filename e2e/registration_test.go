package e2e

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"reduction.dev/reduction-go/connectors/httpapi"
	"reduction.dev/reduction-go/jobs"
	"reduction.dev/reduction/clocks"
	"reduction.dev/reduction/connectors/httpapi/httpapitest"
	"reduction.dev/reduction/jobs/jobstest"
	"reduction.dev/reduction/util/binu"
	"reduction.dev/reduction/workers/workerstest"
)

func TestWorkerRegistrationAfterShutdown(t *testing.T) {
	t.Parallel()

	// Start an http source with two written events
	httpAPIServer := httpapitest.StartServer(httpapitest.WithUnboundedReading())
	defer httpAPIServer.Close()
	httpAPIServer.WriteBatch("events", binu.IntBytesList(1, 1))

	// Start the job server
	jobDef := &jobs.Job{
		WorkerCount:            1,
		WorkingStorageLocation: t.TempDir(),
	}
	source := httpapi.NewSource(jobDef, "Source", &httpapi.SourceParams{
		Addr:     httpAPIServer.URL(),
		Topics:   []string{"events"},
		KeyEvent: KeyEventWithUniformKeyAndZeroTimestamp,
	})
	sink := httpapi.NewSink(jobDef, "Sink", &httpapi.SinkParams{
		Addr: httpAPIServer.URL(),
	})
	operator := jobs.NewOperator(jobDef, "Operator", &jobs.OperatorParams{
		Handler: NewSummingHandler(sink, "sums"),
	})
	source.Connect(operator)
	operator.Connect(sink)

	job, stop := jobstest.Run(jobDef)
	defer stop()

	// Start the handler server
	handlerServer, stop := RunHandler(jobDef)
	defer stop()

	// Start the worker
	worker, stop := workerstest.Run(t, workerstest.NewServerParams{
		HandlerAddr: handlerServer.Addr(),
		JobAddr:     job.RPCAddr(),
	})
	defer stop()

	// Make sure we get two summing events read from the source
	assert.EventuallyWithT(t, func(t *assert.CollectT) {
		assert.Equal(t, 2, httpAPIServer.RecordCount("sums"))
	}, time.Second*1, time.Millisecond*100)

	// Stop the worker
	worker.Stop()

	// Start the worker again
	_, stop = workerstest.Run(t, workerstest.NewServerParams{
		HandlerAddr: handlerServer.Addr(),
		JobAddr:     job.RPCAddr(),
	})
	defer stop()

	// Worker will reread 2 events from source

	// Begin running again
	assert.EventuallyWithT(t, func(t *assert.CollectT) {
		assert.Equal(t, 4, httpAPIServer.RecordCount("sums"))
	}, time.Second*1, time.Millisecond*100)
}

func TestWorkerRegistrationAfterKilled(t *testing.T) {
	t.Parallel()

	// Start an http source with two written events
	httpAPIServer := httpapitest.StartServer(httpapitest.WithUnboundedReading())
	defer httpAPIServer.Close()
	httpAPIServer.WriteBatch("events", binu.IntBytesList(1, 1))

	// Start the job server
	jobDef := &jobs.Job{
		WorkerCount:            1,
		WorkingStorageLocation: t.TempDir(),
	}
	source := httpapi.NewSource(jobDef, "Source", &httpapi.SourceParams{
		Addr:     httpAPIServer.URL(),
		Topics:   []string{"events"},
		KeyEvent: KeyEventWithUniformKeyAndZeroTimestamp,
	})
	sink := httpapi.NewSink(jobDef, "Sink", &httpapi.SinkParams{
		Addr: httpAPIServer.URL(),
	})
	operator := jobs.NewOperator(jobDef, "Operator", &jobs.OperatorParams{
		Handler: NewSummingHandler(sink, "sums"),
	})
	source.Connect(operator)
	operator.Connect(sink)

	clock := clocks.NewFrozenClock()
	job, stop := jobstest.Run(jobDef, jobstest.WithClock(clock))
	defer stop()

	// Start the handler server
	handlerServer, stop := RunHandler(jobDef)
	defer stop()

	// Start the worker
	worker, stop := workerstest.Run(t, workerstest.NewServerParams{
		HandlerAddr: handlerServer.Addr(),
		JobAddr:     job.RPCAddr(),
	})
	defer stop()

	// Make sure we get two summing events read from the source
	assert.EventuallyWithT(t, func(t *assert.CollectT) {
		assert.Equal(t, 2, httpAPIServer.RecordCount("sums"))
	}, time.Second*1, time.Millisecond*100)

	// Stop the worker without allowing deregistration
	worker.Halt()

	// Advance enough to timeout the disconnected worker
	clock.Advance(time.Minute * 1)

	// Start the worker again
	_, stop = workerstest.Run(t, workerstest.NewServerParams{
		HandlerAddr: handlerServer.Addr(),
		JobAddr:     job.RPCAddr(),
	})
	defer stop()

	// Worker will reread 2 events from source

	// Begin running again
	assert.EventuallyWithT(t, func(t *assert.CollectT) {
		assert.Equal(t, 4, httpAPIServer.RecordCount("sums"))
	}, time.Second*1, time.Millisecond*100)
}

func TestAddingStandbyWorker(t *testing.T) {
	t.Parallel()

	// Start an http source with one written event
	httpAPIServer := httpapitest.StartServer(httpapitest.WithUnboundedReading())
	defer httpAPIServer.Close()
	httpAPIServer.WriteBatch("events", binu.IntBytesList(1))

	// Start the job server
	jobDef := &jobs.Job{
		WorkerCount:            1,
		WorkingStorageLocation: t.TempDir(),
	}
	source := httpapi.NewSource(jobDef, "Source", &httpapi.SourceParams{
		Addr:     httpAPIServer.URL(),
		Topics:   []string{"events"},
		KeyEvent: KeyEventWithUniformKeyAndZeroTimestamp,
	})
	sink := httpapi.NewSink(jobDef, "Sink", &httpapi.SinkParams{
		Addr: httpAPIServer.URL(),
	})
	operator := jobs.NewOperator(jobDef, "Operator", &jobs.OperatorParams{
		Handler: NewSummingHandler(sink, "sums"),
	})
	source.Connect(operator)
	operator.Connect(sink)

	clock := clocks.NewFrozenClock()
	job, stop := jobstest.Run(jobDef, jobstest.WithClock(clock))
	defer stop()

	// Start the handler server
	handlerServer, stop := RunHandler(jobDef)
	defer stop()

	// Start the first worker
	_, stop = workerstest.Run(t, workerstest.NewServerParams{
		HandlerAddr: handlerServer.Addr(),
		JobAddr:     job.RPCAddr(),
	})
	defer stop()

	// Get one summing events from the source
	assert.EventuallyWithT(t, func(t *assert.CollectT) {
		assert.Equal(t, 1, httpAPIServer.RecordCount("sums"))
	}, time.Second*1, time.Millisecond*100)

	// Start the second worker, which should be on standby after registering.
	_, stop = workerstest.Run(t, workerstest.NewServerParams{
		HandlerAddr: handlerServer.Addr(),
		JobAddr:     job.RPCAddr(),
	})
	defer stop()

	// Existing worker will read 1 new event from source
	httpAPIServer.WriteBatch("events", binu.IntBytesList(1))
	assert.EventuallyWithT(t, func(t *assert.CollectT) {
		assert.Equal(t, 2, httpAPIServer.RecordCount("sums"))
	}, time.Second*1, time.Millisecond*100)
}
