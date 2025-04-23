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
	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/connectors/connectorstest"
	"reduction.dev/reduction/connectors/kinesis"
	"reduction.dev/reduction/connectors/kinesis/kinesisfake"
	"reduction.dev/reduction/proto/workerpb"
	"reduction.dev/reduction/util/ds"
	"reduction.dev/reduction/util/sliceu"
)

func TestCheckpointing(t *testing.T) {
	kinesisServer, _ := kinesisfake.StartFake()
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

	// Make 20 records but only write half of them
	records := make([]kinesis.Record, 20)
	for i := range records {
		records[i] = kinesis.Record{
			Key:  fmt.Sprintf("key-%d", i),
			Data: fmt.Appendf(nil, "data-%d", i),
		}
	}
	err = client.PutRecordBatch(context.Background(), streamARN, records[:10])
	require.NoError(t, err)

	config := kinesis.SourceConfig{
		StreamARN: streamARN,
		Client:    client,
	}

	// Create source readers
	sourceReaderIDs := []string{"sr1", "sr2"}
	srs := ds.NewSortedMap[string, *kinesis.SourceReader]()
	for _, id := range sourceReaderIDs {
		sr := kinesis.NewSourceReader(config, connectors.SourceReaderHooks{})
		srs.Set(id, sr)
	}

	splitAssignments := connectorstest.AssignmentsFromSplitter(config, sourceReaderIDs)
	for id, sr := range srs.All() {
		sr.AssignSplits(splitAssignments[id])
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

	// Write remaining 10 records
	err = client.PutRecordBatch(context.Background(), streamARN, records[10:])
	require.NoError(t, err)

	// Create new set of source readers
	sourceReaderIDs = []string{"sr3", "sr4"}
	srs = ds.NewSortedMap[string, *kinesis.SourceReader]()
	for _, id := range sourceReaderIDs {
		sr := kinesis.NewSourceReader(config, connectors.SourceReaderHooks{})
		srs.Set(id, sr)
	}

	// Create a source splitter that assigns splits to the source readers
	didAssign := make(chan struct{})
	ss := config.NewSourceSplitter(sourceReaderIDs, connectors.SourceSplitterHooks{
		AssignSplits: func(assignments map[string][]*workerpb.SourceSplit) {
			for id, sr := range srs.All() {
				sr.AssignSplits(assignments[id])
			}
			close(didAssign)
		},
	}, nil)

	// Load the source reader checkpoints
	err = ss.LoadCheckpoints(checkpoints)
	require.NoError(t, err)

	// Start the source splitter and wait for assignments
	ss.Start()
	<-didAssign

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

	assert.Len(t, readEvents, len(records), "all 20 records read")

	recordData := sliceu.Map(records, func(r kinesis.Record) []byte {
		return r.Data
	})
	// Compare with ElementsMatch because the read order is not predictable
	assert.ElementsMatch(t, recordData, readEvents, "read events match those written to kinesis")
}

func TestReadingAfterShardIteratorExpired(t *testing.T) {
	kinesisServer, fake := kinesisfake.StartFake()
	defer kinesisServer.Close()

	client, err := kinesis.NewClient(&kinesis.NewClientParams{
		Endpoint:    kinesisServer.URL,
		Region:      "us-east-2",
		Credentials: aws.AnonymousCredentials{},
	})
	require.NoError(t, err)

	// Create a Kinesis Stream with one shard
	streamARN, err := client.CreateStream(context.Background(), &kinesis.CreateStreamParams{
		StreamName:      "stream-name",
		ShardCount:      1,
		MaxWaitDuration: 1 * time.Minute,
	})
	require.NoError(t, err)

	// Put a record on the stream
	err = client.PutRecordBatch(context.Background(), streamARN, []kinesis.Record{
		{Key: "key", Data: []byte("data")},
	})
	require.NoError(t, err)

	config := kinesis.SourceConfig{
		StreamARN: streamARN,
		Client:    client,
	}

	// Create source reader
	sr := kinesis.NewSourceReader(config, connectors.SourceReaderHooks{})

	assignments := connectorstest.AssignmentsFromSplitter(config, []string{"sr1"})
	err = sr.AssignSplits(assignments["sr1"])
	require.NoError(t, err)

	// Read the events initially
	events, err := sr.ReadEvents()
	require.NoError(t, err)
	assert.Len(t, events, 1, "reads the event")

	// Simulate shard iterator expiration by forcing the fake server to expire iterators
	fake.ExpireShardIterators()

	// Add another record
	err = client.PutRecordBatch(context.Background(), streamARN, []kinesis.Record{
		{Key: "new-key", Data: []byte("new-data")},
	})
	require.NoError(t, err)

	// Try reading again - this should fail because iterator is expired
	events, err = sr.ReadEvents()
	assert.NoError(t, err, "doesn't error despite expired iterator")
	assert.Len(t, events, 1, "reads the event")
}
