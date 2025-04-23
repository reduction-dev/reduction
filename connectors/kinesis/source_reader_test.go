package kinesis_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awskinesis "github.com/aws/aws-sdk-go-v2/service/kinesis"
	kinesistypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/connectors/kinesis"
	"reduction.dev/reduction/connectors/kinesis/kinesisfake"
	"reduction.dev/reduction/connectors/kinesis/kinesispb"
	"reduction.dev/reduction/proto/workerpb"
	"reduction.dev/reduction/util/ptr"
	"reduction.dev/reduction/util/sliceu"
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
	}, connectors.SourceReaderHooks{})
	err = reader.AssignSplits([]*workerpb.SourceSplit{{SplitId: "shardId-000000000000"}})
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
	}, connectors.SourceReaderHooks{})
	err = reader.AssignSplits([]*workerpb.SourceSplit{
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

func TestSourceReader_FinishingShards(t *testing.T) {
	server, _ := kinesisfake.StartFake()
	defer server.Close()

	client, err := kinesis.NewClient(&kinesis.NewClientParams{
		Endpoint:    server.URL,
		Region:      "us-east-2",
		Credentials: aws.AnonymousCredentials{},
		Retryer:     aws.NopRetryer{},
	})
	require.NoError(t, err, "should create Kinesis client")

	streamARN, err := client.CreateStream(t.Context(), &kinesis.CreateStreamParams{
		StreamName:      "stream-name",
		ShardCount:      2,
		MaxWaitDuration: 1 * time.Minute,
	})
	require.NoError(t, err, "should create stream")

	// Put a single record in the stream
	err = client.PutRecordBatch(t.Context(), streamARN, []kinesis.Record{
		{Key: "key", Data: []byte("data")},
	})
	require.NoError(t, err, "should put record batch")

	// List the two shards to get their IDs
	shards, err := client.ListShards(t.Context(), streamARN)
	require.NoError(t, err)
	shardIDs := sliceu.Map(shards, func(s kinesistypes.Shard) string { return *s.ShardId })
	require.Len(t, shardIDs, 2, "should have two shards")

	// Merge the two shards to finish both shards
	_, err = client.MergeShards(t.Context(), &awskinesis.MergeShardsInput{
		StreamARN:            &streamARN,
		ShardToMerge:         shards[0].ShardId,
		AdjacentShardToMerge: shards[1].ShardId,
	})
	require.NoError(t, err, "should merge shards")

	// Create a source reader and assign both of the splits to it
	var splitsFinished []string
	reader := kinesis.NewSourceReader(
		kinesis.SourceConfig{Client: client, StreamARN: streamARN},
		connectors.SourceReaderHooks{
			NotifySplitsFinished: func(ids []string) {
				splitsFinished = append(splitsFinished, ids...)
			},
		},
	)
	err = reader.AssignSplits([]*workerpb.SourceSplit{{SplitId: shardIDs[0]}, {SplitId: shardIDs[1]}})
	require.NoError(t, err, "should set splits")

	// Read records twice to hit both shards
	var allEvents [][]byte
	for range 2 {
		events, err := reader.ReadEvents()
		require.NoError(t, err)
		allEvents = append(allEvents, events...)
	}

	assert.Len(t, allEvents, 1, "should read the one event")
	assert.Equal(t, splitsFinished, shardIDs, "should notify both shards finished")

	// Check that both shard are marked finsished in the checkpoint
	var checkpoint kinesispb.Checkpoint
	require.NoError(t, proto.Unmarshal(reader.Checkpoint(), &checkpoint))
	assert.EqualExportedValues(t, &kinesispb.Checkpoint{
		Shards: []*kinesispb.Shard{{
			ShardId:  shardIDs[0],
			Cursor:   "",
			Finished: true,
		}, {
			ShardId:  shardIDs[1],
			Cursor:   "",
			Finished: true,
		}},
	}, &checkpoint)
}
