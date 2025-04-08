package e2e_test

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clienthttpapi "reduction.dev/reduction-go/connectors/httpapi"
	clientkinesis "reduction.dev/reduction-go/connectors/kinesis"
	"reduction.dev/reduction-go/rxn"
	"reduction.dev/reduction-go/topology"
	"reduction.dev/reduction/connectors/httpapi/httpapitest"
	"reduction.dev/reduction/connectors/kinesis"
	"reduction.dev/reduction/connectors/kinesis/kinesisfake"
	"reduction.dev/reduction/e2e"
	"reduction.dev/reduction/jobs/jobstest"
	"reduction.dev/reduction/util/sliceu"
	"reduction.dev/reduction/workers/workerstest"
)

func TestKinesis(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "aws-acccess-key-id")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "aws-secret-access-key")

	kinesisService := kinesisfake.StartFake()
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
