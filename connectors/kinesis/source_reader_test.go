package kinesis_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	kinesistypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/connectors/kinesis"
	"reduction.dev/reduction/connectors/kinesis/kinesisfake"
	"reduction.dev/reduction/proto/workerpb"
	"reduction.dev/reduction/util/ptr"
)

func TestSourceReader_ProvisionedThroughputExceededIsRetried(t *testing.T) {
	// Start a fake Kinesis server
	server, fake := kinesisfake.StartFake()
	defer server.Close()

	// Create client with anonymous credentials
	client, err := kinesis.NewClient(&kinesis.NewClientParams{
		Endpoint:    server.URL,
		Region:      "us-east-2",
		Credentials: aws.AnonymousCredentials{},
		Retryer:     aws.NopRetryer{},
	})
	require.NoError(t, err, "should create Kinesis client")

	// Create a stream with one shard
	streamARN, err := client.CreateStream(context.Background(), &kinesis.CreateStreamParams{
		StreamName:      "stream-name",
		ShardCount:      1,
		MaxWaitDuration: 1 * time.Minute,
	})
	require.NoError(t, err, "should create stream")

	// Create the source reader and set a split
	reader := kinesis.NewSourceReader(kinesis.SourceConfig{
		Client:    client,
		StreamARN: streamARN,
	})
	err = reader.SetSplits([]*workerpb.SourceSplit{{SplitId: "shardId-000000000000"}})
	require.NoError(t, err, "should set splits")

	// Configure the fake to return a throughput exceeded error
	fake.SetGetRecordsError(&kinesistypes.ProvisionedThroughputExceededException{})

	// Call ReadEvents and confirm we get a retryable throughput exceeded error
	_, err = reader.ReadEvents()
	throughputExceededErr := &kinesistypes.ProvisionedThroughputExceededException{}
	assert.ErrorAs(t, err, &throughputExceededErr, "should return error when throughput exceeded")
	assert.True(t, connectors.IsRetryable(err), "ProvisionedThroughputExceededException should be retryable")

	// Clear the error and verify we can read successfully
	fake.SetGetRecordsError(nil)

	// Put some test data
	err = client.PutRecordBatch(t.Context(), streamARN, []kinesis.Record{
		{Key: "test-key", Data: []byte("test-data")},
	})
	require.NoError(t, err, "should put record batch")

	// Now reading should succeed
	events, err := reader.ReadEvents()
	assert.NoError(t, err, "should read events after error is cleared")
	assert.NotEmpty(t, events, "should return events")
}

func TestSourceReader_AccessDeniedIsTerminal(t *testing.T) {
	// Start a fake Kinesis server
	server, fake := kinesisfake.StartFake()
	defer server.Close()

	// Create client with anonymous credentials
	client, err := kinesis.NewClient(&kinesis.NewClientParams{
		Endpoint:    server.URL,
		Region:      "us-east-2",
		Credentials: aws.AnonymousCredentials{},
		Retryer:     aws.NopRetryer{},
	})
	require.NoError(t, err, "should create Kinesis client")

	// Create a test stream with one shard
	streamARN, err := client.CreateStream(context.Background(), &kinesis.CreateStreamParams{
		StreamName:      "stream-name",
		ShardCount:      1,
		MaxWaitDuration: 1 * time.Minute,
	})
	require.NoError(t, err, "should create stream")

	// Create the source reader with a split
	reader := kinesis.NewSourceReader(kinesis.SourceConfig{
		Client:    client,
		StreamARN: streamARN,
	})
	err = reader.SetSplits([]*workerpb.SourceSplit{
		{SplitId: "shardId-000000000000"},
	})
	assert.NoError(t, err, "should set splits")

	// Set a terminal error
	fake.SetGetRecordsError(&kinesistypes.AccessDeniedException{
		Message: ptr.New("Access denied to the stream"),
	})

	// Read events - this should return an error marked as terminal
	_, err = reader.ReadEvents()
	accessDeniedErr := &kinesistypes.AccessDeniedException{}
	assert.ErrorAs(t, err, &accessDeniedErr, "should return access denied")
	assert.False(t, connectors.IsRetryable(err), "AccessDeniedException should be terminal")
}
