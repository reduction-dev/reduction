package kinesis

import (
	"maps"
	"slices"
	"sync"

	"reduction.dev/reduction/util/ds"
)

type SplitTracker struct {
	knownSplits         *ds.SortedMap[string, SourceSplitterShard]
	assignedSplits      map[string]struct{}
	LastAssignedSplitID string // The last split ID that was marked assigned.
	mu                  sync.Mutex
}

func NewSplitTracker() *SplitTracker {
	return &SplitTracker{
		knownSplits:    ds.NewSortedMap[string, SourceSplitterShard](),
		assignedSplits: make(map[string]struct{}),
	}
}

// LoadSplits loads the initial splits from a checkpoint keeping them unassigned.
func (st *SplitTracker) LoadSplits(shards []SourceSplitterShard, lastAssignedSplitID string) {
	st.mu.Lock()
	defer st.mu.Unlock()

	// Load the splits into the tracker
	for _, shard := range shards {
		st.knownSplits.Set(shard.ShardID, shard)
	}

	st.LastAssignedSplitID = lastAssignedSplitID
}

// AddSplits starts tracking the given kinesis shards.
func (st *SplitTracker) AddSplits(shards []SourceSplitterShard) {
	st.mu.Lock()
	defer st.mu.Unlock()

	for _, shard := range shards {
		st.knownSplits.Set(shard.ShardID, shard)
	}
}

// TrackAssigned marks the given splits as assigned.
func (st *SplitTracker) TrackAssigned(shards []SourceSplitterShard) {
	st.mu.Lock()
	defer st.mu.Unlock()

	for _, shard := range shards {
		st.assignedSplits[shard.ShardID] = struct{}{}
	}

	if len(shards) > 0 {
		st.LastAssignedSplitID = shards[len(shards)-1].ShardID
	}
}

// RemoveSplits removes the given splits from tracking.
func (st *SplitTracker) RemoveSplits(splitIDs []string) {
	st.mu.Lock()
	defer st.mu.Unlock()

	for _, splitID := range splitIDs {
		st.knownSplits.Delete(splitID)
		delete(st.assignedSplits, splitID)
	}
}

// AvailableSplits are not assigned and have no parents that need to be read first.
func (st *SplitTracker) AvailableSplits() []SourceSplitterShard {
	st.mu.Lock()
	defer st.mu.Unlock()

	available := make([]SourceSplitterShard, 0, st.knownSplits.Size())

	for _, split := range st.knownSplits.All() {
		// Skip assigned shards
		_, assigned := st.assignedSplits[split.ShardID]
		if assigned {
			continue
		}

		// Shards with no parents are always available
		if len(split.ParentIDs) == 0 {
			available = append(available, split)
			continue
		}

		// Check if there are any parents that need to be read before this shard
		hasKnownParent := slices.ContainsFunc(split.ParentIDs, func(parentID string) bool {
			_, known := st.knownSplits.Get(parentID)
			return known
		})

		// If no parents are known, the split is available
		if !hasKnownParent {
			available = append(available, split)
		}
	}

	return available
}

func (st *SplitTracker) AssignedSplits() []SourceSplitterShard {
	st.mu.Lock()
	defer st.mu.Unlock()

	assigned := make([]SourceSplitterShard, 0, len(st.assignedSplits))
	for _, shardID := range slices.Sorted(maps.Keys(st.assignedSplits)) {
		knownShard, _ := st.knownSplits.Get(shardID)
		assigned = append(assigned, knownShard)
	}

	return assigned
}
