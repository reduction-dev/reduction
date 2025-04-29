package kinesis

import (
	"maps"
	"slices"
	"sync"
)

type SplitTracker struct {
	knownSplits         map[string]SourceSplitterShard
	assignedSplits      map[string]struct{}
	LastAssignedSplitID string // The last split ID that was marked assigned.
	mu                  sync.Mutex
}

func NewSplitTracker() *SplitTracker {
	return &SplitTracker{
		knownSplits:    make(map[string]SourceSplitterShard),
		assignedSplits: make(map[string]struct{}),
	}
}

// LoadSplits loads the initial splits from a checkpoint marking them as assigned.
func (st *SplitTracker) LoadSplits(shards []SourceSplitterShard, lastAssignedSplitID string) {
	st.mu.Lock()
	defer st.mu.Unlock()

	// Load the splits into the tracker
	for _, shard := range shards {
		st.knownSplits[shard.ShardID] = shard
		st.assignedSplits[shard.ShardID] = struct{}{}
	}

	st.LastAssignedSplitID = lastAssignedSplitID
}

// AddSplits starts tracking the given kinesis shards.
func (st *SplitTracker) AddSplits(shards []SourceSplitterShard) {
	st.mu.Lock()
	defer st.mu.Unlock()

	for _, shard := range shards {
		st.knownSplits[shard.ShardID] = shard
	}
}

// TrackAssigned marks the given splits as assigned.
func (st *SplitTracker) TrackAssigned(shardIDs []string) {
	st.mu.Lock()
	defer st.mu.Unlock()

	for _, shardID := range shardIDs {
		st.assignedSplits[shardID] = struct{}{}
	}

	if len(shardIDs) > 0 {
		st.LastAssignedSplitID = shardIDs[len(shardIDs)-1]
	}
}

// RemoveSplits removes the given splits from tracking.
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
		_, assigned := st.assignedSplits[split.ShardID]
		if assigned {
			continue
		}

		// Shards with no parents are always available
		if len(split.ParentIDs) == 0 {
			available = append(available, split.ShardID)
			continue
		}

		// Check if there are any parents that need to be read before this shard
		hasKnownParent := slices.ContainsFunc(split.ParentIDs, func(parentID string) bool {
			_, known := st.knownSplits[parentID]
			return known
		})

		// If no parents are known, the split is available
		if !hasKnownParent {
			available = append(available, split.ShardID)
		}
	}

	return available
}

func (st *SplitTracker) AssignedSplits() []SourceSplitterShard {
	st.mu.Lock()
	defer st.mu.Unlock()

	assigned := make([]SourceSplitterShard, 0, len(st.assignedSplits))
	for _, shardID := range slices.Sorted(maps.Keys(st.assignedSplits)) {
		assigned = append(assigned, st.knownSplits[shardID])
	}

	return assigned
}
