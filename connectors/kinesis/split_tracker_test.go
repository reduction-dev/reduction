package kinesis_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/stretchr/testify/assert"
	"reduction.dev/reduction/connectors/kinesis"
	"reduction.dev/reduction/util/ptr"
)

func TestSplitTracker_AddSplitsMakesSplitsAvailable(t *testing.T) {
	tracker := kinesis.NewSplitTracker()

	shards := []types.Shard{{ShardId: ptr.New("shard-001")}}
	tracker.AddSplits(shards)

	assert.Equal(t, []string{"shard-001"}, tracker.AvailableSplits(), "should have one available split")
}

func TestSplitTracker_SplitsWithParentsAreNotAvailable(t *testing.T) {
	tracker := kinesis.NewSplitTracker()

	shards := []types.Shard{{
		ShardId:       ptr.New("shard-001"),
		ParentShardId: ptr.New("parent-001"),
	}, {
		ShardId: ptr.New("parent-001"),
	}}
	tracker.AddSplits(shards)

	assert.Equal(t, []string{"parent-001"}, tracker.AvailableSplits(), "should only have parent available")
}

func TestSplitTracker_RemovingParentShardsMakesChildShardsAvailable(t *testing.T) {
	tracker := kinesis.NewSplitTracker()

	shards := []types.Shard{
		{ShardId: ptr.New("shard-001"), ParentShardId: ptr.New("parent-001")},
		{ShardId: ptr.New("parent-001")},
	}
	tracker.AddSplits(shards)

	assert.Equal(t, []string{"parent-001"}, tracker.AvailableSplits(), "should be parent shard")

	tracker.RemoveSplits([]string{"parent-001"})

	assert.Equal(t, []string{"shard-001"}, tracker.AvailableSplits(), "child became available")
}
