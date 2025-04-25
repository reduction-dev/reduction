package e2e_test

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awskinesis "github.com/aws/aws-sdk-go-v2/service/kinesis"
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

	kclient, err := kinesis.NewClient(&kinesis.NewClientParams{
		Endpoint:    kinesisService.URL,
		Region:      "us-east-2",
		Credentials: aws.AnonymousCredentials{},
	})
	require.NoError(t, err)

	// Create a Kinesis Stream with two shards
	streamARN, err := kclient.CreateStream(context.Background(), &kinesis.CreateStreamParams{
		StreamName:      "stream-name",
		ShardCount:      2,
		MaxWaitDuration: 1 * time.Minute,
	})
	require.NoError(t, err)

	// Write 100 records to the stream
	records := make([]kinesis.Record, 100)
	for i := range records {
		records[i] = kinesis.Record{
			Key:  fmt.Sprintf("key-%d", i),
			Data: fmt.Appendf(nil, "data-%d", i),
		}
	}
	err = kclient.PutRecordBatch(context.Background(), streamARN, records)
	require.NoError(t, err)

	sinkServer := httpapitest.StartServer()
	defer sinkServer.Close()

	jobDef := &topology.Job{
		WorkerCount:            topology.IntValue(2),
		WorkingStorageLocation: topology.StringValue(t.TempDir()),
	}
	source := clientkinesis.NewSource(jobDef, "source", &clientkinesis.SourceParams{
		StreamARN: topology.StringValue(streamARN),
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

	kclient, err := kinesis.NewClient(&kinesis.NewClientParams{
		Endpoint:    kinesisService.URL,
		Region:      "us-east-2",
		Credentials: aws.AnonymousCredentials{},
	})
	require.NoError(t, err)

	// Create a Kinesis Stream with two shards
	streamARN, err := kclient.CreateStream(t.Context(), &kinesis.CreateStreamParams{
		StreamName:      "scale-in-stream",
		ShardCount:      2,
		MaxWaitDuration: 1 * time.Minute,
	})
	require.NoError(t, err)

	// Write 100 records to both shards
	records := make([]kinesis.Record, 100)
	for i := range 100 {
		records[i] = kinesis.Record{
			Key:  fmt.Sprintf("key-%03d", i),
			Data: fmt.Appendf(nil, "data-%03d", i),
		}
	}
	err = kclient.PutRecordBatch(t.Context(), streamARN, records)
	require.NoError(t, err)

	sinkServer := httpapitest.StartServer()
	defer sinkServer.Close()

	// User defined job
	jobDef := &topology.Job{
		WorkerCount:            topology.IntValue(2),
		WorkingStorageLocation: topology.StringValue(t.TempDir()),
	}
	source := clientkinesis.NewSource(jobDef, "source", &clientkinesis.SourceParams{
		StreamARN: topology.StringValue(streamARN),
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
	shards, err := kclient.ListShards(t.Context(), streamARN, "")
	require.NoError(t, err)
	require.Len(t, shards, 2)
	_, err = kclient.MergeShards(t.Context(), &awskinesis.MergeShardsInput{
		StreamARN:            &streamARN,
		ShardToMerge:         shards[0].ShardId,
		AdjacentShardToMerge: shards[1].ShardId,
	})
	require.NoError(t, err)

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
	err = kclient.PutRecordBatch(t.Context(), streamARN, moreRecords)
	require.NoError(t, err)

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

	kclient, err := kinesis.NewClient(&kinesis.NewClientParams{
		Endpoint:    kinesisService.URL,
		Region:      "us-east-2",
		Credentials: aws.AnonymousCredentials{},
	})
	require.NoError(t, err)

	// Create a Kinesis Stream with one shard
	streamARN, err := kclient.CreateStream(t.Context(), &kinesis.CreateStreamParams{
		StreamName:      "scale-out-stream",
		ShardCount:      1,
		MaxWaitDuration: 1 * time.Minute,
	})
	require.NoError(t, err)

	// Write 50 records to the single shard
	records := make([]kinesis.Record, 50)
	for i := range records {
		records[i] = kinesis.Record{
			Key:  fmt.Sprintf("key-%03d", i),
			Data: fmt.Appendf(nil, "data-%03d", i),
		}
	}
	err = kclient.PutRecordBatch(t.Context(), streamARN, records)
	require.NoError(t, err)

	sinkServer := httpapitest.StartServer()
	defer sinkServer.Close()

	jobDef := &topology.Job{
		WorkerCount:            topology.IntValue(2),
		WorkingStorageLocation: topology.StringValue(t.TempDir()),
	}
	source := clientkinesis.NewSource(jobDef, "source", &clientkinesis.SourceParams{
		StreamARN: topology.StringValue(streamARN),
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
	shards, err := kclient.ListShards(t.Context(), streamARN, "")
	require.NoError(t, err)
	require.Len(t, shards, 1)

	midpoint := "170141183460469231731687303715884105728" // midpoint of 0 to 2^128-1
	err = kclient.SplitShard(t.Context(), &kinesis.SplitShardParams{
		StreamARN:          streamARN,
		ShardToSplit:       shards[0].ShardId,
		NewStartingHashKey: &midpoint,
	})
	require.NoError(t, err, "split shard should succeed")

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
	err = kclient.PutRecordBatch(t.Context(), streamARN, moreRecords)
	require.NoError(t, err)

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
