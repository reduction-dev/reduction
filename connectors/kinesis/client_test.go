package kinesis_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"reduction.dev/reduction/connectors/kinesis"
	"reduction.dev/reduction/connectors/kinesis/kinesisfake"

	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKinesisAgainstFake(t *testing.T) {
	svc := kinesisfake.StartFake()
	defer svc.Close()

	kclient, err := kinesis.NewClient(&kinesis.NewClientParams{
		Endpoint:    svc.URL,
		Region:      "us-east-2",
		Credentials: credentials.NewStaticCredentialsProvider("key", "secret", "session"),
	})
	require.NoError(t, err)

	testWritingEvents(t, context.Background(), kclient, "test-stream")
}

func TestKinesisAgainstAWS(t *testing.T) {
	if os.Getenv("INTEGRATION") == "" && os.Getenv("INTEGRATION_KINESIS") == "" {
		t.Skip("integration-only")
	}

	kclient, err := kinesis.NewClient(&kinesis.NewClientParams{
		Region:  "us-east-2",
		Profile: "mitchlloyd",
	})
	require.NoError(t, err)

	testWritingEvents(t, context.Background(), kclient, "integration-test-stream")
}

func testWritingEvents(t *testing.T, ctx context.Context, kclient *kinesis.Client, streamName string) {
	err := kclient.DeleteStream(ctx, &kinesis.DeleteStreamParams{
		StreamName:      streamName,
		MaxWaitDuration: 1 * time.Minute,
	})
	if err == nil {
		slog.Info("deleted existing test stream")
	}

	streamARN, err := kclient.CreateStream(ctx, &kinesis.CreateStreamParams{
		StreamName:      streamName,
		ShardCount:      2,
		MaxWaitDuration: 1 * time.Minute,
	})
	require.NoError(t, err)
	defer func() {
		err := kclient.DeleteStream(ctx, &kinesis.DeleteStreamParams{
			StreamName:      streamName,
			MaxWaitDuration: 1 * time.Minute,
		})
		require.NoError(t, err)
	}()

	writeEvents := make([]kinesis.Record, 10)
	for i := range writeEvents {
		writeEvents[i] = kinesis.Record{
			Key:  fmt.Sprintf("key-%d", i),
			Data: fmt.Appendf(nil, "data-%d", i),
		}
	}
	err = kclient.PutRecordBatch(context.Background(), streamARN, writeEvents)
	require.NoError(t, err)

	shardIDs, err := kclient.ListShards(ctx, streamARN)
	require.NoError(t, err)

	assert.Equal(t, []string{"shardId-000000000000", "shardId-000000000001"}, shardIDs)

	eventBatch1, err := kclient.ReadEvents(ctx, streamARN, shardIDs[0], "")
	require.NoError(t, err)

	var shard1Data []string
	for _, e := range eventBatch1.Records {
		shard1Data = append(shard1Data, string(e.Data))
	}
	assert.Equal(t, []string{"data-1", "data-3", "data-7"}, shard1Data)

	eventBatch2, err := kclient.ReadEvents(ctx, streamARN, shardIDs[1], "")
	require.NoError(t, err)

	var shard2Data []string
	for _, e := range eventBatch2.Records {
		shard2Data = append(shard2Data, string(e.Data))
	}
	assert.Equal(t, []string{"data-0", "data-2", "data-4", "data-5", "data-6", "data-8", "data-9"}, shard2Data)
}
