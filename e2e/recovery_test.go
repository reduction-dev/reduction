package e2e

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"reduction.dev/reduction-go/connectors/httpapi"
	"reduction.dev/reduction-go/rxn"
	"reduction.dev/reduction-go/topology"
	"reduction.dev/reduction/clocks"
	"reduction.dev/reduction/connectors/httpapi/httpapitest"
	"reduction.dev/reduction/jobs/jobstest"
	"reduction.dev/reduction/rpc"
	"reduction.dev/reduction/storage/locations"
	"reduction.dev/reduction/util/binu"
	"reduction.dev/reduction/util/iteru"
	"reduction.dev/reduction/workers/workerstest"
)

func TestRestartFromSavepoint(t *testing.T) {
	t.Parallel()

	// Start an http source and write some events to it.
	httpAPIServer := httpapitest.StartServer(httpapitest.WithUnboundedReading())
	defer httpAPIServer.Close()
	httpAPIServer.WriteBatch("events", binu.IntBytesList(1, 1))

	// Start the job server
	testDir := t.TempDir()
	jobDef := &topology.Job{
		WorkerCount:              topology.IntValue(1),
		WorkingStorageLocation:   topology.StringValue(t.TempDir()),
		SavepointStorageLocation: topology.StringValue(filepath.Join(testDir, "savepoints")),
	}
	source := httpapi.NewSource(jobDef, "Source", &httpapi.SourceParams{
		Addr:     topology.StringValue(httpAPIServer.URL()),
		Topics:   []string{"events"},
		KeyEvent: KeyEventWithUniformKeyAndZeroTimestamp,
	})
	sink := httpapi.NewSink(jobDef, "Sink", &httpapi.SinkParams{
		Addr: topology.StringValue(httpAPIServer.URL()),
	})
	operator := topology.NewOperator(jobDef, "Operator", &topology.OperatorParams{
		Handler: func(op *topology.Operator) rxn.OperatorHandler {
			return NewSummingHandler(sink, "sums", op)
		},
	})
	source.Connect(operator)
	operator.Connect(sink)

	// Start the job server
	jobStore, err := locations.New(t.TempDir())
	require.NoError(t, err)
	job, stop := jobstest.Run(jobDef, jobstest.WithStore(jobStore))
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

	readResp := httpAPIServer.Read("sums")
	assert.Equal(t, binu.IntBytesList(1, 2), readResp.Events)

	// Request a savepoint
	ui := rpc.NewJobUIConnectClient(job.RESTAddr(), connect.WithProtoJSON())
	ctx := context.Background()
	savepointID, err := ui.CreateSavepoint(ctx)
	require.NoError(t, err)

	// Retrieve the savepoint
	var savepointURI string
	assert.EventuallyWithT(t, func(t *assert.CollectT) {
		savepointURI, err = ui.GetSavepoint(ctx, savepointID)
		assert.NoError(t, err)
	}, time.Second, time.Millisecond)

	// Stop the cluster
	job.Stop()
	worker.Stop()
	handlerServer.Stop(context.Background())

	// Remove all working files but not the savepoint
	err = os.RemoveAll(jobDef.WorkingStorageLocation.Proto().GetValue())
	require.NoError(t, err)

	// Start up the job server again with savepoint URI
	job, stop = jobstest.Run(jobDef, jobstest.WithSavepoint(savepointURI), jobstest.WithStore(jobStore))
	defer stop()

	// Start the handler again
	handlerServer, stop = RunHandler(jobDef)
	defer stop()

	// Start the worker again
	_, stop = workerstest.Run(t, workerstest.NewServerParams{
		HandlerAddr: handlerServer.Addr(),
		JobAddr:     job.RPCAddr(),
	})
	defer stop()

	// Put some more events in the source
	httpAPIServer.WriteBatch("events", binu.IntBytesList(1, 1))

	// Make sure we get two more summing events (source stars from correct cursor)
	assert.EventuallyWithT(t, func(t *assert.CollectT) {
		assert.Equal(t, 4, httpAPIServer.RecordCount("sums"))
	}, time.Second*1, time.Millisecond*100)

	// Check that the state recovered
	readResp = httpAPIServer.Read("sums")
	assert.Equal(t, binu.IntBytesList(1, 2, 3, 4), readResp.Events)
}

func TestRestartWorkerFromInMemoryJobCheckpoint(t *testing.T) {
	t.Parallel()

	// Start an http source and write an event to it.
	httpAPIServer := httpapitest.StartServer(httpapitest.WithUnboundedReading())
	defer httpAPIServer.Close()
	httpAPIServer.WriteBatch("events", binu.IntBytesList(1))

	// Start the job server
	jobDef := &topology.Job{
		WorkerCount:            topology.IntValue(1),
		WorkingStorageLocation: topology.StringValue(t.TempDir()),
	}
	source := httpapi.NewSource(jobDef, "Source", &httpapi.SourceParams{
		Addr:     topology.StringValue(httpAPIServer.URL()),
		Topics:   []string{"events"},
		KeyEvent: KeyEventWithUniformKeyAndZeroTimestamp,
	})
	sink := httpapi.NewSink(jobDef, "Sink", &httpapi.SinkParams{
		Addr: topology.StringValue(httpAPIServer.URL()),
	})
	operator := topology.NewOperator(jobDef, "Operator", &topology.OperatorParams{
		Handler: func(op *topology.Operator) rxn.OperatorHandler {
			return NewSummingHandler(sink, "sums", op)
		},
	})
	source.Connect(operator)
	operator.Connect(sink)

	clock := clocks.NewFrozenClock()
	jobStore := locations.NewLocalDirectory(filepath.Join(t.TempDir(), "job"))
	job, stop := jobstest.Run(jobDef, jobstest.WithClock(clock), jobstest.WithStore(jobStore))
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

	// Make sure we get one summing event into the sink
	assert.EventuallyWithT(t, func(t *assert.CollectT) {
		assert.Equal(t, 1, httpAPIServer.RecordCount("sums"))
	}, time.Second*1, time.Millisecond*100)

	// The sum in the sink is "1"
	assert.Equal(t, binu.IntBytesList(1), httpAPIServer.Read("sums").Events)

	// Checkpoint
	fsEvents := jobStore.Subscribe()
	clock.TickEvery("checkpointing")
	fileCreated := <-fsEvents
	assert.Equal(t, locations.OpCreate, fileCreated.Op)
	assert.Contains(t, fileCreated.Path, ".snapshot")

	// Just stop the worker.
	worker.Stop()

	// Start a new worker
	_, stop = workerstest.Run(t, workerstest.NewServerParams{
		HandlerAddr: handlerServer.Addr(),
		JobAddr:     job.RPCAddr(),
	})
	defer stop()

	// Write another event to the source
	httpAPIServer.WriteBatch("events", binu.IntBytesList(1))

	// Should eventually get 2 sums in the sink
	assert.EventuallyWithT(t, func(t *assert.CollectT) {
		assert.Equal(t, 2, httpAPIServer.RecordCount("sums"))
	}, time.Second*1, time.Millisecond*100)

	// The second sum should be 2
	assert.Equal(t, binu.IntBytesList(1, 2), httpAPIServer.Read("sums").Events)
}

func TestScaleOutWorkers(t *testing.T) {
	t.Parallel()

	// Start an http source and write 10 events to it.
	httpAPIServer := httpapitest.StartServer(httpapitest.WithUnboundedReading())
	defer httpAPIServer.Close()
	for range iteru.Times(10) {
		httpAPIServer.Write("events", binu.IntBytes(1))
	}

	// Start the job server
	jobDef := &topology.Job{
		WorkerCount:            topology.IntValue(1),
		WorkingStorageLocation: topology.StringValue(t.TempDir()),
	}
	source := httpapi.NewSource(jobDef, "Source", &httpapi.SourceParams{
		Addr:     topology.StringValue(httpAPIServer.URL()),
		Topics:   []string{"events"},
		KeyEvent: KeyEventWithUniformKeyAndZeroTimestamp,
	})
	sink := httpapi.NewSink(jobDef, "Sink", &httpapi.SinkParams{
		Addr: topology.StringValue(httpAPIServer.URL()),
	})
	operator := topology.NewOperator(jobDef, "Operator", &topology.OperatorParams{
		Handler: func(op *topology.Operator) rxn.OperatorHandler {
			return NewSummingHandler(sink, "sums", op)
		},
	})
	source.Connect(operator)
	operator.Connect(sink)

	clock := clocks.NewFrozenClock()
	testDir := t.TempDir()
	jobStore := locations.NewLocalDirectory(filepath.Join(testDir, "job"))
	job, stop := jobstest.Run(jobDef, jobstest.WithClock(clock), jobstest.WithStore(jobStore))
	defer stop()

	// Start the handler server
	handlerServer, stop := RunHandler(jobDef)
	defer stop()

	// Start the worker
	worker1, stop := workerstest.Run(t, workerstest.NewServerParams{
		LogPrefix:   "worker-1",
		HandlerAddr: handlerServer.Addr(),
		JobAddr:     job.RPCAddr(),
	})
	defer stop()

	// Make sure we get 10 summing events into the sink
	assert.EventuallyWithT(t, func(t *assert.CollectT) {
		assert.Equal(t, 10, httpAPIServer.RecordCount("sums"))
	}, time.Second*1, time.Millisecond*100)

	// The sum in the sink is "10"
	sumEvents := httpAPIServer.Read("sums").Events
	assert.Equal(t, binu.IntBytes(10), sumEvents[len(sumEvents)-1])

	// Checkpoint
	fsEvents := jobStore.Subscribe()
	clock.TickEvery("checkpointing")
	fileCreate := <-fsEvents
	assert.Equal(t, locations.OpCreate, fileCreate.Op)
	assert.Contains(t, fileCreate.Path, ".snapshot")

	// Stop the cluster
	worker1.Stop()
	job.Stop()

	// Add 10 more events to the source
	for range iteru.Times(10) {
		httpAPIServer.Write("events", binu.IntBytes(1))
	}

	// Start the job server again but with 2 workers
	jobDef.WorkerCount = topology.IntValue(2)
	job, stop = jobstest.Run(jobDef, jobstest.WithClock(clock), jobstest.WithStore(jobStore))
	defer stop()

	// Start 2 new workers
	_, stop = workerstest.Run(t, workerstest.NewServerParams{
		LogPrefix:   "worker-2",
		HandlerAddr: handlerServer.Addr(),
		JobAddr:     job.RPCAddr(),
	})
	defer stop()

	_, stop = workerstest.Run(t, workerstest.NewServerParams{
		LogPrefix:   "worker-3",
		HandlerAddr: handlerServer.Addr(),
		JobAddr:     job.RPCAddr(),
	})
	defer stop()

	// Make sure we get 20 summing events into the sink. 10 are from the first job
	// and 10 are from the second job.
	assert.EventuallyWithT(t, func(t *assert.CollectT) {
		assert.Equal(t, 20, httpAPIServer.RecordCount("sums"))
	}, time.Second*1, time.Millisecond*100)

	// The sum in the sink is "20"
	sumEvents = httpAPIServer.Read("sums").Events
	assert.Equal(t, binu.IntBytes(20), sumEvents[len(sumEvents)-1])
}

func TestScaleInWorkers(t *testing.T) {
	t.Parallel()

	// Start an http source and write 10 events to it.
	httpAPIServer := httpapitest.StartServer(httpapitest.WithUnboundedReading())
	defer httpAPIServer.Close()
	for range iteru.Times(10) {
		httpAPIServer.Write("events", binu.IntBytes(1))
	}

	// Start the job server for two workers
	testDir := t.TempDir()
	jobDef := &topology.Job{
		WorkerCount:            topology.IntValue(2),
		WorkingStorageLocation: topology.StringValue(testDir),
	}
	source := httpapi.NewSource(jobDef, "Source", &httpapi.SourceParams{
		Addr:     topology.StringValue(httpAPIServer.URL()),
		Topics:   []string{"events"},
		KeyEvent: KeyEventWithUniformKeyAndZeroTimestamp,
	})
	sink := httpapi.NewSink(jobDef, "Sink", &httpapi.SinkParams{
		Addr: topology.StringValue(httpAPIServer.URL()),
	})
	operator := topology.NewOperator(jobDef, "Operator", &topology.OperatorParams{
		Handler: func(op *topology.Operator) rxn.OperatorHandler {
			return NewSummingHandler(sink, "sums", op)
		},
	})
	source.Connect(operator)
	operator.Connect(sink)

	clock := clocks.NewFrozenClock()
	jobStore := locations.NewLocalDirectory(filepath.Join(testDir, "job"))
	job, stop := jobstest.Run(jobDef, jobstest.WithClock(clock), jobstest.WithStore(jobStore))
	defer stop()

	// Start the handler server
	handlerServer, stop := RunHandler(jobDef)
	defer stop()

	// Start the first worker
	worker1, stop := workerstest.Run(t, workerstest.NewServerParams{
		LogPrefix:   "worker-1",
		HandlerAddr: handlerServer.Addr(),
		JobAddr:     job.RPCAddr(),
		Clock:       clock,
	})
	defer stop()

	// Start the second worker
	worker2, stop := workerstest.Run(t, workerstest.NewServerParams{
		LogPrefix:   "worker-2",
		HandlerAddr: handlerServer.Addr(),
		JobAddr:     job.RPCAddr(),
		Clock:       clock,
	})
	defer stop()

	// Make sure we get 10 summing events into the sink
	assert.EventuallyWithT(t, func(t *assert.CollectT) {
		assert.Equal(t, 10, httpAPIServer.RecordCount("sums"))
	}, time.Second*1, time.Millisecond*100)

	// The sum in the sink is "10"
	sumEvents := httpAPIServer.Read("sums").Events
	assert.Equal(t, binu.IntBytes(10), sumEvents[len(sumEvents)-1])

	// Checkpoint
	fsEvents := jobStore.Subscribe()
	clock.TickEvery("checkpointing")
	fileCreated := <-fsEvents
	assert.Equal(t, locations.OpCreate, fileCreated.Op)
	assert.Contains(t, fileCreated.Path, ".snapshot")

	// Stop the cluster and the second worker
	job.Stop()
	worker1.Stop()
	worker2.Stop()

	// Add 10 more events to the source
	for range iteru.Times(10) {
		httpAPIServer.Write("events", binu.IntBytes(1))
	}

	// Start the job server again but with 1 worker
	jobDef.WorkerCount = topology.IntValue(1)
	job, stop = jobstest.Run(jobDef, jobstest.WithClock(clock), jobstest.WithStore(jobStore))
	defer stop()

	// Start the third worker
	_, stop = workerstest.Run(t, workerstest.NewServerParams{
		LogPrefix:   "worker-3",
		HandlerAddr: handlerServer.Addr(),
		JobAddr:     job.RPCAddr(),
	})
	defer stop()

	// Make sure we get 20 summing events into the sink. 10 are from the first job
	// and 10 are from the second job.
	assert.EventuallyWithT(t, func(t *assert.CollectT) {
		assert.Equal(t, 20, httpAPIServer.RecordCount("sums"))
	}, time.Second*1, time.Millisecond*100, "correct total number of emitted events")

	// The last sum in the sink is "20"
	sumEvents = httpAPIServer.Read("sums").Events
	assert.Equal(t, 20, int(binary.BigEndian.Uint64(sumEvents[len(sumEvents)-1])))
}

func TestRestartFromLatestOfTwoCheckpoints(t *testing.T) {
	t.Parallel()

	// Start an http source and write 10 events to it.
	httpAPIServer := httpapitest.StartServer(httpapitest.WithUnboundedReading())
	defer httpAPIServer.Close()
	for range iteru.Times(10) {
		httpAPIServer.Write("events", binu.IntBytes(1))
	}

	// Start the job server
	testDir := t.TempDir()
	jobDef := &topology.Job{
		WorkerCount:            topology.IntValue(1),
		WorkingStorageLocation: topology.StringValue(testDir),
	}
	source := httpapi.NewSource(jobDef, "Source", &httpapi.SourceParams{
		Addr:     topology.StringValue(httpAPIServer.URL()),
		Topics:   []string{"events"},
		KeyEvent: KeyEventWithUniformKeyAndZeroTimestamp,
	})
	sink := httpapi.NewSink(jobDef, "Sink", &httpapi.SinkParams{
		Addr: topology.StringValue(httpAPIServer.URL()),
	})
	operator := topology.NewOperator(jobDef, "Operator", &topology.OperatorParams{
		Handler: func(op *topology.Operator) rxn.OperatorHandler {
			return NewSummingHandler(sink, "sums", op)
		},
	})
	source.Connect(operator)
	operator.Connect(sink)

	clock := clocks.NewFrozenClock()
	jobStore := locations.NewLocalDirectory(filepath.Join(testDir, "job"))
	job, stop := jobstest.Run(jobDef, jobstest.WithClock(clock), jobstest.WithStore(jobStore))
	defer stop()

	// Start the handler server
	handlerServer, stop := RunHandler(jobDef)
	defer stop()

	// Start the first worker
	worker, stop := workerstest.Run(t, workerstest.NewServerParams{
		HandlerAddr: handlerServer.Addr(),
		JobAddr:     job.RPCAddr(),
		Clock:       clock,
	})
	defer stop()

	// Make sure we get 10 summing events into the sink
	assert.EventuallyWithT(t, func(t *assert.CollectT) {
		assert.Equal(t, 10, httpAPIServer.RecordCount("sums"))
	}, time.Second*1, time.Millisecond*100)

	// The sum in the sink is "10"
	sumEvents := httpAPIServer.Read("sums").Events
	assert.Equal(t, binu.IntBytes(10), sumEvents[len(sumEvents)-1])

	// Checkpoint
	fsEvents := jobStore.Subscribe()
	clock.TickEvery("checkpointing")
	fileCreated := <-fsEvents
	assert.Equal(t, fileCreated.Op, locations.OpCreate)
	assert.Contains(t, fileCreated.Path, ".snapshot")

	// Add 10 more events to the source
	for range iteru.Times(10) {
		httpAPIServer.Write("events", binu.IntBytes(1))
	}

	// Make sure we get 20 summing events into the sink. 10 are from the first job
	// and 10 are from the second job.
	assert.EventuallyWithT(t, func(t *assert.CollectT) {
		assert.Equal(t, 20, httpAPIServer.RecordCount("sums"))
	}, time.Second*1, time.Millisecond*100, "correct total number of emitted events")

	// The last sum in the sink is "20"
	sumEvents = httpAPIServer.Read("sums").Events
	assert.Equal(t, 20, int(binary.BigEndian.Uint64(sumEvents[len(sumEvents)-1])))

	// Checkpoint again
	clock.TickEvery("checkpointing")
	fileCreated = <-fsEvents
	assert.Equal(t, fileCreated.Op, locations.OpCreate)
	assert.Contains(t, fileCreated.Path, ".snapshot")

	// Stop the cluster and the second worker
	worker.Stop()
	job.Stop()

	// Add 10 more events to the source
	for range iteru.Times(10) {
		httpAPIServer.Write("events", binu.IntBytes(1))
	}

	// Start the job server again
	job, stop = jobstest.Run(jobDef, jobstest.WithClock(clock), jobstest.WithStore(jobStore))
	defer stop()

	// Start the next worker
	_, stop = workerstest.Run(t, workerstest.NewServerParams{
		HandlerAddr: handlerServer.Addr(),
		JobAddr:     job.RPCAddr(),
	})
	defer stop()

	// Make sure we get 30 summing events into the sink.
	assert.EventuallyWithT(t, func(t *assert.CollectT) {
		assert.Equal(t, 30, httpAPIServer.RecordCount("sums"))
	}, time.Second*1, time.Millisecond*100, "correct total number of emitted events")

	// The last sum in the sink is "30"
	sumEvents = httpAPIServer.Read("sums").Events
	assert.Equal(t, 30, int(binary.BigEndian.Uint64(sumEvents[len(sumEvents)-1])))
}
