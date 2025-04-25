package kinesis_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"reduction.dev/reduction-protocol/kinesispb"
	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/connectors/connectorstest"
	"reduction.dev/reduction/connectors/kinesis"
	"reduction.dev/reduction/connectors/kinesis/kinesisfake"
	"reduction.dev/reduction/proto/snapshotpb"
	"reduction.dev/reduction/proto/workerpb"
	"reduction.dev/reduction/util/ds"
	"reduction.dev/reduction/util/sliceu"
)

func TestCheckpointing(t *testing.T) {
	kinesisServer, _ := kinesisfake.StartFake()
	defer kinesisServer.Close()

	client := kinesis.NewLocalClient(kinesisServer.URL)
	stream := kinesis.CreateTempStream(t, client, 2)

	// Make 20 records but only write half of them
	records := make([]kinesis.Record, 20)
	for i := range records {
		records[i] = kinesis.Record{
			Key:  fmt.Sprintf("key-%d", i),
			Data: fmt.Appendf(nil, "data-%d", i),
		}
	}
	stream.PutRecordBatch(t, records[:10])

	config := kinesis.SourceConfig{
		StreamARN: stream.StreamARN,
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
			var record kinesispb.Record
			err := proto.Unmarshal(event, &record)
			require.NoError(t, err)
			readEvents = append(readEvents, record.Data)
		}
	}

	// Checkpoint
	checkpoint := &snapshotpb.SourceCheckpoint{}
	for _, sr := range srs.All() {
		checkpoint.SplitStates = append(checkpoint.SplitStates, sr.Checkpoint()...)
	}

	// Write remaining 10 records
	stream.PutRecordBatch(t, records[10:])

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
	err := ss.LoadCheckpoint(checkpoint)
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
			var record kinesispb.Record
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

	client := kinesis.NewLocalClient(kinesisServer.URL)
	stream := kinesis.CreateTempStream(t, client, 1)

	// Put a record on the stream
	stream.PutRecordBatch(t, []kinesis.Record{
		{Key: "key", Data: []byte("data")},
	})

	config := kinesis.SourceConfig{
		StreamARN: stream.StreamARN,
		Client:    client,
	}

	// Create source reader
	sr := kinesis.NewSourceReader(config, connectors.SourceReaderHooks{})

	assignments := connectorstest.AssignmentsFromSplitter(config, []string{"sr1"})
	err := sr.AssignSplits(assignments["sr1"])
	require.NoError(t, err)

	// Read the events initially
	events, err := sr.ReadEvents()
	require.NoError(t, err)
	assert.Len(t, events, 1, "reads the event")

	// Simulate shard iterator expiration by forcing the fake server to expire iterators
	fake.ExpireShardIterators()

	// Add another record
	stream.PutRecordBatch(t, []kinesis.Record{
		{Key: "new-key", Data: []byte("new-data")},
	})

	// Try reading again - this should fail because iterator is expired
	events, err = sr.ReadEvents()
	assert.NoError(t, err, "doesn't error despite expired iterator")
	assert.Len(t, events, 1, "reads the event")
}
