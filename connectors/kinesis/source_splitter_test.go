package kinesis_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"reduction.dev/reduction/connectors/connectorstest"
	"reduction.dev/reduction/connectors/kinesis"
	"reduction.dev/reduction/connectors/kinesis/kinesisfake"
	"reduction.dev/reduction/proto/workerpb"
)

func TestSourceSplitter_AssignsSplitsUniformly(t *testing.T) {
	fakeServer, _ := kinesisfake.StartFake()
	defer fakeServer.Close()

	client := kinesis.NewLocalClient(fakeServer.URL)
	stream := kinesis.CreateTempStream(t, client, 4) // 4 shards

	config := kinesis.SourceConfig{
		StreamARN: stream.StreamARN,
		Client:    client,
	}

	runnerIDs := []string{"runnerA", "runnerB"}
	assignments := connectorstest.AssignmentsFromSplitter(config, runnerIDs)

	assert.EqualExportedValues(t, map[string][]*workerpb.SourceSplit{
		"runnerA": {
			{SplitId: "shardId-000000000000", SourceId: "tbd", Cursor: []byte{}},
			{SplitId: "shardId-000000000001", SourceId: "tbd", Cursor: []byte{}},
		},
		"runnerB": {
			{SplitId: "shardId-000000000002", SourceId: "tbd", Cursor: []byte{}},
			{SplitId: "shardId-000000000003", SourceId: "tbd", Cursor: []byte{}},
		},
	}, assignments)
}

func TestSourceSplitter_UniformAssignmentAfterSplits(t *testing.T) {
	fakeServer, _ := kinesisfake.StartFake()
	defer fakeServer.Close()

	client := kinesis.NewLocalClient(fakeServer.URL)
	stream := kinesis.CreateTempStream(t, client, 2) // Start with 2 shards

	runnerIDs := []string{"runnerA", "runnerB"}
	const discoveryInterval = 1 * time.Millisecond
	config := kinesis.SourceConfig{
		StreamARN:              stream.StreamARN,
		Client:                 client,
		ShardDiscoveryInterval: discoveryInterval,
	}

	// Initial assignment: 2 shards across 2 runners
	splitter, assignmentsChannel := connectorstest.StartSplitterAssignments(config, runnerIDs)
	assignments := <-assignmentsChannel
	assert.EqualExportedValues(t, map[string][]*workerpb.SourceSplit{
		"runnerA": {
			{SplitId: "shardId-000000000000", SourceId: "tbd", Cursor: []byte{}},
		},
		"runnerB": {
			{SplitId: "shardId-000000000001", SourceId: "tbd", Cursor: []byte{}},
		},
	}, assignments, "initial shards assigned")

	// Split the first shard
	shardIDs := stream.ShardIDs
	midpoint := "85070591730234615865843651857942052864" // 2^127 / 2
	stream.SplitShard(t, shardIDs[0], midpoint)

	// Assigning after removing a shard
	splitter.NotifySplitsFinished("runnerA", []string{"shardId-000000000000"})
	assignments = <-assignmentsChannel
	assert.EqualExportedValues(t, map[string][]*workerpb.SourceSplit{
		"runnerA": {
			{SplitId: "shardId-000000000002", SourceId: "tbd", Cursor: []byte{}},
			{SplitId: "shardId-000000000003", SourceId: "tbd", Cursor: []byte{}},
		},
	}, assignments, "new shards assigned")
}
