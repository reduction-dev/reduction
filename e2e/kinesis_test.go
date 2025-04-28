package e2e_test

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clienthttpapi "reduction.dev/reduction-go/connectors/httpapi"
	clientkinesis "reduction.dev/reduction-go/connectors/kinesis"
	"reduction.dev/reduction-go/rxn"
	"reduction.dev/reduction-go/topology"
	"reduction.dev/reduction/clocks"
	"reduction.dev/reduction/connectors/httpapi/httpapitest"
	"reduction.dev/reduction/connectors/kinesis"
	"reduction.dev/reduction/connectors/kinesis/kinesisfake"
	"reduction.dev/reduction/e2e"
	"reduction.dev/reduction/jobs/jobstest"
	"reduction.dev/reduction/storage/locations"
	"reduction.dev/reduction/util/sliceu"
	"reduction.dev/reduction/workers/workerstest"
)

func TestKinesis(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "aws-acccess-key-id")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "aws-secret-access-key")

	kinesisService, _ := kinesisfake.StartFake()
	defer kinesisService.Close()

	client := kinesis.NewLocalClient(kinesisService.URL)
	stream := kinesis.CreateTempStream(t, client, 2)

	// Write 100 records to the stream
	records := make([]kinesis.Record, 100)
	for i := range records {
		records[i] = kinesis.Record{
			Key:  fmt.Sprintf("key-%d", i),
			Data: fmt.Appendf(nil, "data-%d", i),
		}
	}
	stream.PutRecordBatch(t, records)

	sinkServer := httpapitest.StartServer()
	defer sinkServer.Close()

	jobDef := &topology.Job{
		WorkerCount:            topology.IntValue(2),
		WorkingStorageLocation: topology.StringValue(t.TempDir()),
	}
	source := clientkinesis.NewSource(jobDef, "source", &clientkinesis.SourceParams{
		StreamARN: topology.StringValue(stream.StreamARN),
		Endpoint:  topology.StringValue(kinesisService.URL),
		KeyEvent:  e2e.KeyKinesisEventWithRawKeyAndZeroTimestamp,
	})
	sink := clienthttpapi.NewSink(jobDef, "sink", &clienthttpapi.SinkParams{
		Addr: topology.StringValue(sinkServer.URL()),
	})
	operator := topology.NewOperator(jobDef, "Operator", &topology.OperatorParams{
		Handler: func(op *topology.Operator) rxn.OperatorHandler {
			return e2e.NewPassThroughHandler(sink, "main")
		},
	})
	source.Connect(operator)
	operator.Connect(sink)

	job, stop := jobstest.Run(jobDef)
	defer stop()

	handlerServer, stop := e2e.RunHandler(jobDef)
	defer stop()

	_, stop = workerstest.Run(t, workerstest.NewServerParams{
		HandlerAddr: handlerServer.Addr(),
		JobAddr:     job.RPCAddr(),
	})
	defer stop()

	_, stop = workerstest.Run(t, workerstest.NewServerParams{
		HandlerAddr: handlerServer.Addr(),
		JobAddr:     job.RPCAddr(),
	})
	defer stop()

	require.EventuallyWithT(t, func(t *assert.CollectT) {
		assert.Equal(t, 100, sinkServer.RecordCount("main"), "httpapi sink has 100 items")
	}, 5*time.Second, 100*time.Millisecond)

	sinkRecords := sinkServer.Read("main").Events
	slices.SortFunc(sinkRecords, func(a, b []byte) int {
		return cmp.Compare(string(a), string(b))
	})
	assert.Equal(t, []byte("data-0"), sinkRecords[0])
	assert.Equal(t, []byte("data-99"), sliceu.Last(sinkRecords))
}

func TestKinesis_ScaleIn(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "aws-acccess-key-id")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "aws-secret-access-key")

	kinesisService, _ := kinesisfake.StartFake()
	defer kinesisService.Close()

	client := kinesis.NewLocalClient(kinesisService.URL)
	stream := kinesis.CreateTempStream(t, client, 2)

	// Write 100 records to both shards
	records := make([]kinesis.Record, 100)
	for i := range 100 {
		records[i] = kinesis.Record{
			Key:  fmt.Sprintf("key-%03d", i),
			Data: fmt.Appendf(nil, "data-%03d", i),
		}
	}
	stream.PutRecordBatch(t, records)

	sinkServer := httpapitest.StartServer()
	defer sinkServer.Close()

	// User defined job
	jobDef := &topology.Job{
		WorkerCount:            topology.IntValue(2),
		WorkingStorageLocation: topology.StringValue(t.TempDir()),
	}
	source := clientkinesis.NewSource(jobDef, "source", &clientkinesis.SourceParams{
		StreamARN: topology.StringValue(stream.StreamARN),
		Endpoint:  topology.StringValue(kinesisService.URL),
		KeyEvent:  e2e.KeyKinesisEventWithRawKeyAndZeroTimestamp,
	})
	sink := clienthttpapi.NewSink(jobDef, "sink", &clienthttpapi.SinkParams{
		Addr: topology.StringValue(sinkServer.URL()),
	})
	operator := topology.NewOperator(jobDef, "Operator", &topology.OperatorParams{
		Handler: func(op *topology.Operator) rxn.OperatorHandler {
			return e2e.NewPassThroughHandler(sink, "main")
		},
	})
	source.Connect(operator)
	operator.Connect(sink)

	frozenClock := clocks.NewFrozenClock()
	jobStore := locations.NewLocalDirectory(t.TempDir())
	job, stopJob := jobstest.Run(jobDef, jobstest.WithClock(frozenClock), jobstest.WithStore(jobStore))

	handlerServer, stop := e2e.RunHandler(jobDef)
	defer stop()

	_, stopWorker1 := workerstest.Run(t, workerstest.NewServerParams{
		HandlerAddr: handlerServer.Addr(),
		JobAddr:     job.RPCAddr(),
	})

	_, stopWorker2 := workerstest.Run(t, workerstest.NewServerParams{
		HandlerAddr: handlerServer.Addr(),
		JobAddr:     job.RPCAddr(),
	})

	// Wait for all 100 events to be read
	assert.Eventually(t, func() bool {
		return sinkServer.RecordCount("main") == 100
	}, 2*time.Second, 10*time.Millisecond, "httpapi sink has 100 items after initial write")

	// Merge the two shards into one (scale-in)
	shardIDs := stream.ShardIDs
	assert.Len(t, shardIDs, 2)
	stream.MergeShards(t, shardIDs[0], shardIDs[1])

	// Checkpoint
	fsEvents := jobStore.Subscribe()
	frozenClock.TickEvery("checkpointing")
	fileCreated := <-fsEvents
	assert.Equal(t, locations.OpCreate, fileCreated.Op)
	assert.Contains(t, fileCreated.Path, ".snapshot")

	// Stop the cluster
	stopJob()
	stopWorker1()
	stopWorker2()

	// Write 10 more records to the now-single-shard stream
	moreRecords := make([]kinesis.Record, 10)
	for i := range 10 {
		moreRecords[i] = kinesis.Record{
			Key:  fmt.Sprintf("key-%03d", 100+i),
			Data: fmt.Appendf(nil, "data-%03d", 100+i),
		}
	}
	stream.PutRecordBatch(t, moreRecords)

	// Restart from checkpoint (re-run job and workers)
	job, stop = jobstest.Run(jobDef, jobstest.WithStore(jobStore))
	defer stop()

	handlerServer, stop = e2e.RunHandler(jobDef)
	defer stop()

	_, stop = workerstest.Run(t, workerstest.NewServerParams{
		HandlerAddr: handlerServer.Addr(),
		JobAddr:     job.RPCAddr(),
	})
	defer stop()

	_, stop = workerstest.Run(t, workerstest.NewServerParams{
		HandlerAddr: handlerServer.Addr(),
		JobAddr:     job.RPCAddr(),
	})
	defer stop()

	// Wait for the 10 new events to be read (should not re-read the first 100)
	assert.Eventually(t, func() bool {
		return sinkServer.RecordCount("main") == 110
	}, 2*time.Second, 100*time.Millisecond, "httpapi sink has 110 items after scale-in and restart")

	sinkRecords := sinkServer.Read("main").Events
	slices.SortFunc(sinkRecords, func(a, b []byte) int {
		return cmp.Compare(string(a), string(b))
	})
	assert.Equal(t, []byte("data-000"), sinkRecords[0], "first event is data-000")
	assert.Equal(t, []byte("data-109"), sliceu.Last(sinkRecords), "last event is data-109")
}

func TestKinesis_ScaleOut(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "aws-acccess-key-id")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "aws-secret-access-key")

	kinesisService, _ := kinesisfake.StartFake()
	defer kinesisService.Close()

	client := kinesis.NewLocalClient(kinesisService.URL)
	stream := kinesis.CreateTempStream(t, client, 1)

	// Write 50 records to the single shard
	records := make([]kinesis.Record, 50)
	for i := range records {
		records[i] = kinesis.Record{
			Key:  fmt.Sprintf("key-%03d", i),
			Data: fmt.Appendf(nil, "data-%03d", i),
		}
	}
	stream.PutRecordBatch(t, records)

	sinkServer := httpapitest.StartServer()
	defer sinkServer.Close()

	jobDef := &topology.Job{
		WorkerCount:            topology.IntValue(2),
		WorkingStorageLocation: topology.StringValue(t.TempDir()),
	}
	source := clientkinesis.NewSource(jobDef, "source", &clientkinesis.SourceParams{
		StreamARN: topology.StringValue(stream.StreamARN),
		Endpoint:  topology.StringValue(kinesisService.URL),
		KeyEvent:  e2e.KeyKinesisEventWithRawKeyAndZeroTimestamp,
	})
	sink := clienthttpapi.NewSink(jobDef, "sink", &clienthttpapi.SinkParams{
		Addr: topology.StringValue(sinkServer.URL()),
	})
	operator := topology.NewOperator(jobDef, "Operator", &topology.OperatorParams{
		Handler: func(op *topology.Operator) rxn.OperatorHandler {
			return e2e.NewPassThroughHandler(sink, "main")
		},
	})
	source.Connect(operator)
	operator.Connect(sink)

	frozenClock := clocks.NewFrozenClock()
	jobStore := locations.NewLocalDirectory(t.TempDir())
	job, stopJob := jobstest.Run(jobDef, jobstest.WithClock(frozenClock), jobstest.WithStore(jobStore))

	handlerServer, stop := e2e.RunHandler(jobDef)
	defer stop()

	_, stopWorker1 := workerstest.Run(t, workerstest.NewServerParams{
		HandlerAddr: handlerServer.Addr(),
		JobAddr:     job.RPCAddr(),
	})
	_, stopWorker2 := workerstest.Run(t, workerstest.NewServerParams{
		HandlerAddr: handlerServer.Addr(),
		JobAddr:     job.RPCAddr(),
	})

	// Wait for all 50 events to be read
	assert.Eventually(t, func() bool {
		return sinkServer.RecordCount("main") == 50
	}, 2*time.Second, 10*time.Millisecond, "httpapi sink has 50 items after initial write")

	// Split the single shard into two (scale-out)
	shardIDs := stream.ShardIDs
	assert.Len(t, shardIDs, 1)
	midpoint := "170141183460469231731687303715884105728" // midpoint of 0 to 2^128-1
	stream.SplitShard(t, shardIDs[0], midpoint)

	// Checkpoint
	fsEvents := jobStore.Subscribe()
	frozenClock.TickEvery("checkpointing")
	fileCreated := <-fsEvents
	assert.Equal(t, locations.OpCreate, fileCreated.Op)
	assert.Contains(t, fileCreated.Path, ".snapshot")

	// Stop the cluster
	stopJob()
	stopWorker1()
	stopWorker2()

	// Write 20 more records to the now-two-shard stream
	moreRecords := make([]kinesis.Record, 20)
	for i := range moreRecords {
		moreRecords[i] = kinesis.Record{
			Key:  fmt.Sprintf("key-%03d", 50+i),
			Data: fmt.Appendf(nil, "data-%03d", 50+i),
		}
	}
	stream.PutRecordBatch(t, moreRecords)

	// Restart from checkpoint (re-run job and workers)
	job, stop = jobstest.Run(jobDef, jobstest.WithStore(jobStore))
	defer stop()

	handlerServer, stop = e2e.RunHandler(jobDef)
	defer stop()

	_, stop = workerstest.Run(t, workerstest.NewServerParams{
		HandlerAddr: handlerServer.Addr(),
		JobAddr:     job.RPCAddr(),
	})
	defer stop()

	_, stop = workerstest.Run(t, workerstest.NewServerParams{
		HandlerAddr: handlerServer.Addr(),
		JobAddr:     job.RPCAddr(),
	})
	defer stop()

	// Wait for the 20 new events to be read (should not re-read the first 50)
	assert.Eventually(t, func() bool {
		return sinkServer.RecordCount("main") == 70
	}, 5*time.Second, 100*time.Millisecond, "httpapi sink has 70 items after scale-out and restart")

	sinkRecords := sinkServer.Read("main").Events
	slices.SortFunc(sinkRecords, func(a, b []byte) int {
		return cmp.Compare(string(a), string(b))
	})
	assert.Equal(t, []byte("data-000"), sinkRecords[0], "first event is data-000")
	assert.Equal(t, []byte("data-069"), sliceu.Last(sinkRecords), "last event is data-069")
}

func TestKinesis_ShardLineage(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "aws-acccess-key-id")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "aws-secret-access-key")

	kinesisService, fake := kinesisfake.StartFake()
	fake.SetGetRecordsLimit(1) // Read 1 record at a time
	defer kinesisService.Close()

	// Create a kinesis stream with 1 shard
	client := kinesis.NewLocalClient(kinesisService.URL)
	stream := kinesis.CreateTempStream(t, client, 1)
	latestShardIDs := stream.ShardIDs

	batchSize := 10

	// Write 10 events to the stream
	firstRecords := make([]kinesis.Record, batchSize)
	for i := range firstRecords {
		firstRecords[i] = kinesis.Record{
			Key:  fmt.Sprintf("key-%02d", i),
			Data: fmt.Appendf(nil, "data-%02d", i),
		}
	}
	stream.PutRecordBatch(t, firstRecords)

	// Split to 2 shards
	midpoint := "170141183460469231731687303715884105728" // midpoint of 0 to 2^128-1
	stream.SplitShard(t, latestShardIDs[0], midpoint)
	latestShardIDs = stream.ListShards(t, latestShardIDs[0])
	require.Len(t, latestShardIDs, 2)

	// Write 10 events to the stream
	secondRecords := make([]kinesis.Record, batchSize)
	for i := range secondRecords {
		secondRecords[i] = kinesis.Record{
			Key:  fmt.Sprintf("key-%02d", batchSize+i),
			Data: fmt.Appendf(nil, "data-%02d", batchSize+i),
		}
	}
	stream.PutRecordBatch(t, secondRecords)

	// Merge back to 1 shard
	stream.MergeShards(t, latestShardIDs[0], latestShardIDs[1])
	latestShardIDs = stream.ListShards(t, latestShardIDs[1])
	require.Len(t, latestShardIDs, 1)

	// Write 10 events to the stream
	thirdRecords := make([]kinesis.Record, batchSize)
	for i := range thirdRecords {
		thirdRecords[i] = kinesis.Record{
			Key:  fmt.Sprintf("key-%02d", (2*batchSize)+i),
			Data: fmt.Appendf(nil, "data-%02d", (2*batchSize)+i),
		}
	}
	stream.PutRecordBatch(t, thirdRecords)

	// Start a job with 1 worker
	sinkServer := httpapitest.StartServer()
	defer sinkServer.Close()

	jobDef := &topology.Job{
		WorkerCount:            topology.IntValue(1),
		WorkingStorageLocation: topology.StringValue(t.TempDir()),
	}
	source := clientkinesis.NewSource(jobDef, "source", &clientkinesis.SourceParams{
		StreamARN: topology.StringValue(stream.StreamARN),
		Endpoint:  topology.StringValue(kinesisService.URL),
		KeyEvent: func(ctx context.Context, record *clientkinesis.Record) ([]rxn.KeyedEvent, error) {
			return []rxn.KeyedEvent{{
				Key:   []byte("static-key"), // static key to make asserting on order possible
				Value: record.Data,
			}}, nil
		},
	})
	sink := clienthttpapi.NewSink(jobDef, "sink", &clienthttpapi.SinkParams{
		Addr: topology.StringValue(sinkServer.URL()),
	})
	operator := topology.NewOperator(jobDef, "Operator", &topology.OperatorParams{
		Handler: func(op *topology.Operator) rxn.OperatorHandler {
			return e2e.NewPassThroughHandler(sink, "main")
		},
	})
	source.Connect(operator)
	operator.Connect(sink)

	job, stop := jobstest.Run(jobDef)
	defer stop()

	handlerServer, stop := e2e.RunHandler(jobDef)
	defer stop()

	_, stop = workerstest.Run(t, workerstest.NewServerParams{
		HandlerAddr: handlerServer.Addr(),
		JobAddr:     job.RPCAddr(),
	})
	defer stop()

	// Wait for all events to be read
	assert.EventuallyWithT(t, func(t *assert.CollectT) {
		require.Equal(t, 3*batchSize, sinkServer.RecordCount("main"))
	}, 2*time.Second, 10*time.Millisecond, "httpapi sink has 30 items after all writes")

	// Assert event order constraints
	sinkRecords := sinkServer.Read("main").Events

	// First 10 events must be in order
	firstSet := sinkRecords[:batchSize]
	firstExpected := make([][]byte, batchSize)
	for i := range batchSize {
		firstExpected[i] = fmt.Appendf(nil, "data-%02d", i)
	}
	assert.Equal(t, firstExpected, firstSet, "first 10 events in order at index")

	// Next 10 events: order may vary within this set
	secondSet := sinkRecords[batchSize : 2*batchSize]
	secondExpected := make([][]byte, batchSize)
	for i := range batchSize {
		secondExpected[i] = fmt.Appendf(nil, "data-%02d", batchSize+i)
	}
	assert.ElementsMatch(t, secondExpected, secondSet, "second 10 events match, order may vary")

	// Last 10 events must be in order
	lastSet := sinkRecords[2*batchSize : 3*batchSize]
	lastExpected := make([][]byte, batchSize)
	for i := range batchSize {
		lastExpected[i] = fmt.Appendf(nil, "data-%02d", (2*batchSize)+i)
	}
	assert.Equal(t, lastExpected, lastSet, "last 10 events in order at index")
}
