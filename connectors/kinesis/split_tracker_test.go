package kinesis_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"reduction.dev/reduction/connectors/kinesis"
)

func TestSplitTracker_AddSplitsMakesSplitsAvailable(t *testing.T) {
	tracker := kinesis.NewSplitTracker()

	shards := []kinesis.SourceSplitterShard{{ShardID: "shard-001"}}
	tracker.AddSplits(shards)

	assert.Equal(t, []kinesis.SourceSplitterShard{{ShardID: "shard-001"}}, tracker.AvailableSplits(), "should have one available split")
}

func TestSplitTracker_SplitsWithParentsAreNotAvailable(t *testing.T) {
	tracker := kinesis.NewSplitTracker()

	shards := []kinesis.SourceSplitterShard{{
		ShardID:   "shard-001",
		ParentIDs: []string{"parent-001"},
	}, {
		ShardID: "parent-001",
	}}
	tracker.AddSplits(shards)

	assert.Equal(t, []kinesis.SourceSplitterShard{{ShardID: "parent-001"}}, tracker.AvailableSplits(), "should only have parent available")
}

func TestSplitTracker_RemovingParentShardsMakesChildShardsAvailable(t *testing.T) {
	tracker := kinesis.NewSplitTracker()

	child := kinesis.SourceSplitterShard{ShardID: "shard-001", ParentIDs: []string{"parent-001"}}
	parent := kinesis.SourceSplitterShard{ShardID: "parent-001"}
	shards := []kinesis.SourceSplitterShard{child, parent}
	tracker.AddSplits(shards)

	assert.Equal(t, []kinesis.SourceSplitterShard{parent}, tracker.AvailableSplits(), "should be parent shard")

	tracker.RemoveSplits([]string{"parent-001"})

	assert.Equal(t, []kinesis.SourceSplitterShard{{ShardID: "shard-001", ParentIDs: []string{"parent-001"}}}, tracker.AvailableSplits(), "child became available")
}

func TestSplitTracker_LastAssignedSplitID(t *testing.T) {
	tracker := kinesis.NewSplitTracker()

	// LoadSplits sets LastAssignedSplitID to the last loaded split
	shards := []kinesis.SourceSplitterShard{
		{ShardID: "shard-1"},
		{ShardID: "shard-2"},
	}
	tracker.LoadSplits(shards, "shard-2")
	assert.Equal(t, "shard-2", tracker.LastAssignedSplitID, "should set LastAssignedSplitID to the last loaded split")

	// TrackAssigned updates LastAssignedSplitID to the last assigned split
	moreShards := []kinesis.SourceSplitterShard{
		{ShardID: "shard-3"},
		{ShardID: "shard-4"},
	}
	tracker.AddSplits(moreShards)
	tracker.TrackAssigned([]kinesis.SourceSplitterShard{{ShardID: "shard-3"}, {ShardID: "shard-4"}})
	assert.Equal(t, "shard-4", tracker.LastAssignedSplitID, "should update LastAssignedSplitID to the last assigned split")

	// RemoveSplits does not change LastAssignedSplitID
	tracker.RemoveSplits([]string{"shard-4"})
	assert.Equal(t, "shard-4", tracker.LastAssignedSplitID, "RemoveSplits should not change LastAssignedSplitID")
}

func TestSplitTracker_AssignedSplits(t *testing.T) {
	tracker := kinesis.NewSplitTracker()

	shards := []kinesis.SourceSplitterShard{{ShardID: "a"}, {ShardID: "b"}}
	tracker.LoadSplits(shards, "")
	assert.Empty(t, tracker.AssignedSplits(), "has no assigned splits after loading")

	tracker.TrackAssigned(shards)
	assigned := tracker.AssignedSplits()
	assert.Equal(t, []kinesis.SourceSplitterShard{{ShardID: "a"}, {ShardID: "b"}}, assigned, "should return all loaded splits as assigned")

	// AddSplits does not assign
	tracker.AddSplits([]kinesis.SourceSplitterShard{{ShardID: "c"}})
	assigned = tracker.AssignedSplits()
	assert.Equal(t, []kinesis.SourceSplitterShard{{ShardID: "a"}, {ShardID: "b"}}, assigned, "AddSplits should not assign new splits")

	// TrackAssigned adds to assignedSplits
	tracker.TrackAssigned([]kinesis.SourceSplitterShard{{ShardID: "c"}})
	assigned = tracker.AssignedSplits()
	assert.Equal(t, []kinesis.SourceSplitterShard{{ShardID: "a"}, {ShardID: "b"}, {ShardID: "c"}}, assigned, "TrackAssigned should add to assignedSplits")

	// RemoveSplits removes from assignedSplits
	tracker.RemoveSplits([]string{"b"})
	assigned = tracker.AssignedSplits()
	assert.Equal(t, []kinesis.SourceSplitterShard{{ShardID: "a"}, {ShardID: "c"}}, assigned, "RemoveSplits should remove from assignedSplits")
}
