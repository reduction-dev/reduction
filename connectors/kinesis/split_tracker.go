package kinesis

import (
	"slices"
	"sync"

	awstypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

type SplitTracker struct {
	knownSplits    map[string]KinesisShard
	assignedSplits map[string]struct{}
	mu             sync.Mutex
}

type KinesisShard struct {
	ShardId        string
	ParentShardIDs []string
}

func NewSplitTracker() *SplitTracker {
	return &SplitTracker{
		knownSplits:    make(map[string]KinesisShard),
		assignedSplits: make(map[string]struct{}),
	}
}

// AddSplits starts tracking the given kinesis shards.
func (st *SplitTracker) AddSplits(kinesisShards []awstypes.Shard) {
	st.mu.Lock()
	defer st.mu.Unlock()

	for _, kShard := range kinesisShards {
		parentIDs := make([]string, 0, 2)
		if kShard.ParentShardId != nil {
			parentIDs = append(parentIDs, *kShard.ParentShardId)
		}
		if kShard.AdjacentParentShardId != nil {
			parentIDs = append(parentIDs, *kShard.AdjacentParentShardId)
		}
		st.knownSplits[*kShard.ShardId] = KinesisShard{
			ShardId:        *kShard.ShardId,
			ParentShardIDs: parentIDs,
		}
	}
}

// TrackAssigned marks the given splits as assigned.
func (st *SplitTracker) TrackAssigned(shardIDs []string) {
	st.mu.Lock()
	defer st.mu.Unlock()

	for _, shardID := range shardIDs {
		st.assignedSplits[shardID] = struct{}{}
	}
}

// RemoveSlpits removes the given splits from tracking.
func (st *SplitTracker) RemoveSplits(splitIDs []string) {
	st.mu.Lock()
	defer st.mu.Unlock()

	for _, splitID := range splitIDs {
		delete(st.knownSplits, splitID)
		delete(st.assignedSplits, splitID)
	}
}

// AvailableSplits are not assigned and have no parents that need to be read first.
func (st *SplitTracker) AvailableSplits() []string {
	st.mu.Lock()
	defer st.mu.Unlock()

	available := make([]string, 0, len(st.knownSplits))

	for _, split := range st.knownSplits {
		// Skip assigned shards
		_, assigned := st.assignedSplits[split.ShardId]
		if assigned {
			continue
		}

		// Shards with no parents are always available
		if len(split.ParentShardIDs) == 0 {
			available = append(available, split.ShardId)
			continue
		}

		// Check if there are any parents that need to be read before this shard
		hasKnownParent := slices.ContainsFunc(split.ParentShardIDs, func(parentID string) bool {
			_, known := st.knownSplits[parentID]
			return known
		})

		// If no parents are known, the split is available
		if !hasKnownParent {
			available = append(available, split.ShardId)
		}
	}

	return available
}
