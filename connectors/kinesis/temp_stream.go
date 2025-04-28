package kinesis

import (
	"context"
	"fmt"
	"testing"
	"time"

	"reduction.dev/reduction/util/ptr"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/stretchr/testify/require"
)

type Record struct {
	Key  string
	Data []byte
}

// TempStream provides helpers for creating, interacting with, then deleting
// Kinesis streams. It's used for testing.
type TempStream struct {
	StreamName string
	StreamARN  string
	ShardIDs   []string
	Client     *kinesis.Client
}

var tempStreamID = 0

var tempStreamMaxWaitDuration = 30 * time.Second

func CreateTempStream(t *testing.T, client *kinesis.Client, shardCount int) *TempStream {
	ctx := context.Background()
	streamName := fmt.Sprintf("temp-stream-%d", tempStreamID)

	_, err := client.CreateStream(ctx, &kinesis.CreateStreamInput{
		StreamName: &streamName,
		ShardCount: ptr.New(int32(shardCount)),
	})
	require.NoError(t, err, "create stream")

	describeInput := &kinesis.DescribeStreamInput{StreamName: &streamName}
	w := kinesis.NewStreamExistsWaiter(client)
	err = w.Wait(ctx, describeInput, tempStreamMaxWaitDuration)
	require.NoError(t, err, "create stream waiting")

	describeStreamOut, err := client.DescribeStream(ctx, describeInput)
	require.NoError(t, err, "describe stream")

	listShardsOut, err := client.ListShards(ctx, &kinesis.ListShardsInput{
		StreamName: &streamName,
	})
	require.NoError(t, err, "list shards")

	shardIDs := make([]string, len(listShardsOut.Shards))
	for i, shard := range listShardsOut.Shards {
		shardIDs[i] = *shard.ShardId
	}

	require.NotNil(t, describeStreamOut.StreamDescription, "stream description should not be nil")

	return &TempStream{
		StreamName: streamName,
		StreamARN:  *describeStreamOut.StreamDescription.StreamARN,
		ShardIDs:   shardIDs,
		Client:     client,
	}
}

func (s *TempStream) Delete(t *testing.T) {
	input := &kinesis.DeleteStreamInput{StreamName: &s.StreamName}
	_, err := s.Client.DeleteStream(t.Context(), input)
	require.NoError(t, err, "delete stream")

	w := kinesis.NewStreamNotExistsWaiter(s.Client)
	err = w.Wait(
		t.Context(),
		&kinesis.DescribeStreamInput{StreamName: &s.StreamName},
		tempStreamMaxWaitDuration,
		func(snewo *kinesis.StreamNotExistsWaiterOptions) {
			snewo.MinDelay = 1 * time.Millisecond
		},
	)
	require.NoError(t, err, "delete stream waiting")
}

func (s *TempStream) MergeShards(t *testing.T, left, right string) {
	_, err := s.Client.MergeShards(t.Context(), &kinesis.MergeShardsInput{
		StreamName:           &s.StreamName,
		ShardToMerge:         &left,
		AdjacentShardToMerge: &right,
	})
	require.NoError(t, err, "merge shards")
}

func (s *TempStream) SplitShard(t *testing.T, shardID string, newHashKey string) {
	input := &kinesis.SplitShardInput{
		StreamName:         &s.StreamName,
		ShardToSplit:       &shardID,
		NewStartingHashKey: &newHashKey,
	}
	_, err := s.Client.SplitShard(t.Context(), input)
	require.NoError(t, err, "split shard")
}

func (s *TempStream) PutRecordBatch(t *testing.T, records []Record) {
	entries := make([]types.PutRecordsRequestEntry, len(records))
	for i, e := range records {
		entries[i] = types.PutRecordsRequestEntry{Data: e.Data, PartitionKey: &e.Key}
	}
	recordBatch := &kinesis.PutRecordsInput{
		Records:   entries,
		StreamARN: &s.StreamARN,
	}
	_, err := s.Client.PutRecords(t.Context(), recordBatch)
	require.NoError(t, err, "put record batch")
}

// ListShards returns the list of shard IDs, optionally starting after a given shard ID.
func (s *TempStream) ListShards(t *testing.T, afterShardID string) []string {
	input := &kinesis.ListShardsInput{
		StreamName: &s.StreamName,
	}
	if afterShardID != "" {
		input.ExclusiveStartShardId = &afterShardID
	}
	resp, err := s.Client.ListShards(context.Background(), input)
	require.NoError(t, err, "list shards")

	shardIDs := make([]string, len(resp.Shards))
	for i, shard := range resp.Shards {
		shardIDs[i] = *shard.ShardId
	}
	return shardIDs
}

func NewLocalClient(endpoint string) *kinesis.Client {
	cfg := kinesis.Options{
		EndpointResolver: kinesis.EndpointResolverFromURL(endpoint),
		Region:           "us-east-2",
		Credentials:      aws.AnonymousCredentials{},
		Retryer:          aws.NopRetryer{},
	}
	return kinesis.New(cfg)
}
