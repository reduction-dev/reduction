package e2e_test

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	rxn "reduction.dev/reduction-go/jobs"
	cfg "reduction.dev/reduction/config"
	"reduction.dev/reduction/connectors/httpapi/httpapitest"
	"reduction.dev/reduction/connectors/kinesis"
	"reduction.dev/reduction/connectors/kinesis/kinesisfake"
	"reduction.dev/reduction/e2e"
	"reduction.dev/reduction/jobs/jobstest"
	"reduction.dev/reduction/util/sliceu"
	"reduction.dev/reduction/workers/workerstest"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKinesis(t *testing.T) {
	t.Parallel()

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
			Data: []byte(fmt.Sprintf("data-%d", i)),
		}
	}
	err = kclient.PutRecordBatch(context.Background(), streamARN, records)
	require.NoError(t, err)

	sinkServer := httpapitest.StartServer()
	defer sinkServer.Close()

	gfConfig := rxn.NewJob("job", &rxn.JobParams{
		WorkerCount:            2,
		WorkingStorageLocation: t.TempDir(),
		Sources: []rxn.Source{rxn.NewKinesisSource("source", &rxn.KinesisSourceParams{
			StreamARN: streamARN,
			Endpoint:  kinesisService.URL,
		})},
		Sinks: []rxn.Sink{rxn.NewHTTPAPISink("sink", &rxn.HTTPAPISinkParams{
			Addr: sinkServer.URL(),
		})},
	})

	config, err := cfg.Unmarshal(gfConfig.Marshal())
	require.NoError(t, err)

	job, stop := jobstest.Run(config)
	defer stop()

	handler := e2e.NewPassThroughHandler("sink", "main")
	handlerServer, stop := e2e.RunHandler(handler)
	defer stop()

	_, stop = workerstest.Run(workerstest.NewServerParams{
		HandlerAddr: handlerServer.Addr(),
		JobAddr:     job.RPCAddr(),
	})
	defer stop()

	_, stop = workerstest.Run(workerstest.NewServerParams{
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
