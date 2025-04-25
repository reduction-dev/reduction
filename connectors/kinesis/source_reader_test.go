package kinesis_test

import (
	"testing"

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

	client := kinesis.NewLocalClient(server.URL)
	stream := kinesis.CreateTempStream(t, client, 1)

	// Create the source reader and set a split
	reader := kinesis.NewSourceReader(kinesis.SourceConfig{
		Client:    client,
		StreamARN: stream.StreamARN,
	}, connectors.SourceReaderHooks{})
	err := reader.AssignSplits([]*workerpb.SourceSplit{{SplitId: stream.ShardIDs[0]}})
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
	stream.PutRecordBatch(t, []kinesis.Record{
		{Key: "test-key", Data: []byte("test-data")},
	})

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
	client := kinesis.NewLocalClient(server.URL)
	stream := kinesis.CreateTempStream(t, client, 1)

	// Create the source reader with a split
	reader := kinesis.NewSourceReader(kinesis.SourceConfig{
		Client:    client,
		StreamARN: stream.StreamARN,
	}, connectors.SourceReaderHooks{})
	err := reader.AssignSplits([]*workerpb.SourceSplit{{SplitId: stream.ShardIDs[0]}})
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

	client := kinesis.NewLocalClient(server.URL)
	stream := kinesis.CreateTempStream(t, client, 2)

	// Put a single record in the stream
	stream.PutRecordBatch(t, []kinesis.Record{
		{Key: "k1", Data: []byte("v1")},
	})

	shardIDs := stream.ShardIDs
	require.Len(t, shardIDs, 2, "should have two shards")

	// Merge the two shards to finish both shards
	stream.MergeShards(t, shardIDs[0], shardIDs[1])

	// Create a source reader and assign both of the splits to it
	var splitsFinished []string
	reader := kinesis.NewSourceReader(
		kinesis.SourceConfig{Client: client, StreamARN: stream.StreamARN},
		connectors.SourceReaderHooks{
			NotifySplitsFinished: func(ids []string) {
				splitsFinished = append(splitsFinished, ids...)
			},
		},
	)
	err := reader.AssignSplits([]*workerpb.SourceSplit{{SplitId: shardIDs[0]}, {SplitId: shardIDs[1]}})
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

	// Check that both finished shards are absent from the checkpoint
	splitStates := reader.Checkpoint()
	assert.Len(t, splitStates, 0, "should not have any split states in checkpoint")
}
