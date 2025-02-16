package kinesis_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	protocol "reduction.dev/reduction-protocol/kinesispb"
	"reduction.dev/reduction/connectors/kinesis"
	"reduction.dev/reduction/connectors/kinesis/kinesisfake"
	"reduction.dev/reduction/util/ds"
	"reduction.dev/reduction/util/sliceu"
)

func TestCheckpointing(t *testing.T) {
	kinesisServer := kinesisfake.StartFake()
	defer kinesisServer.Close()

	client, err := kinesis.NewClient(&kinesis.NewClientParams{
		Endpoint:    kinesisServer.URL,
		Region:      "us-east-2",
		Credentials: aws.AnonymousCredentials{},
	})
	require.NoError(t, err)

	// Create a Kinesis Stream with two shards
	streamARN, err := client.CreateStream(context.Background(), &kinesis.CreateStreamParams{
		StreamName:      "stream-name",
		ShardCount:      2,
		MaxWaitDuration: 1 * time.Minute,
	})
	require.NoError(t, err)

	// Make 100 records but only write half of them
	records := make([]kinesis.Record, 100)
	for i := range records {
		records[i] = kinesis.Record{
			Key:  fmt.Sprintf("key-%d", i),
			Data: []byte(fmt.Sprintf("data-%d", i)),
		}
	}
	err = client.PutRecordBatch(context.Background(), streamARN, records[:50])
	require.NoError(t, err)

	config := kinesis.SourceConfig{
		StreamARN: streamARN,
		Client:    client,
	}

	// Create source readers
	sourceReaderIDs := []string{"sr1", "sr2"}
	srs := ds.NewSortedMap[string, *kinesis.SourceReader]()
	for _, id := range sourceReaderIDs {
		sr := kinesis.NewSourceReader(config)
		srs.Set(id, sr)
	}

	// Assign splits to source readers
	ss := config.NewSourceSplitter()
	splitAssignments, err := ss.AssignSplits(sourceReaderIDs)
	require.NoError(t, err)
	for id, sr := range srs.All() {
		sr.SetSplits(splitAssignments[id])
	}

	// Read half the events from the splits
	var readEvents [][]byte
	for _, sr := range srs.All() {
		events, err := sr.ReadEvents()
		require.NoError(t, err)

		// Unmarshal each protobuf record to get the raw data
		for _, event := range events {
			var record protocol.Record
			err := proto.Unmarshal(event, &record)
			require.NoError(t, err)
			readEvents = append(readEvents, record.Data)
		}
	}

	// Checkpoint
	checkpoints := make([][]byte, 0, srs.Size())
	for _, sr := range srs.All() {
		checkpoints = append(checkpoints, sr.Checkpoint())
	}

	// Write remaining 50 records
	err = client.PutRecordBatch(context.Background(), streamARN, records[50:])
	require.NoError(t, err)

	// Create new set of source readers
	sourceReaderIDs = []string{"sr3", "sr4"}
	srs = ds.NewSortedMap[string, *kinesis.SourceReader]()
	for _, id := range sourceReaderIDs {
		sr := kinesis.NewSourceReader(config)
		srs.Set(id, sr)
	}

	// Assign splits to new source readers
	err = ss.LoadCheckpoints(checkpoints)
	require.NoError(t, err)
	splitAssignments, err = ss.AssignSplits(sourceReaderIDs)
	require.NoError(t, err)
	for id, sr := range srs.All() {
		sr.SetSplits(splitAssignments[id])
	}

	// Read newly written events from the splits
	for _, sr := range srs.All() {
		events, err := sr.ReadEvents()
		require.NoError(t, err)

		// Unmarshal each protobuf record to get the raw data
		for _, event := range events {
			var record protocol.Record
			err := proto.Unmarshal(event, &record)
			require.NoError(t, err)
			readEvents = append(readEvents, record.Data)
		}
	}

	assert.Len(t, readEvents, len(records), "all 100 records read")

	recordData := sliceu.Map(records, func(r kinesis.Record) []byte {
		return r.Data
	})
	// Compare with ElementsMatch because the read order is not predictable
	assert.ElementsMatch(t, recordData, readEvents, "read events match those written to kinesis")
}
