package partitioning_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"reduction.dev/reduction/partitioning"
)

func TestKeyGroupRangeOverlaps(t *testing.T) {
	kgr := partitioning.KeyGroupRange{Start: 2, End: 5}

	// Overlapping cases
	assert.True(t, kgr.Overlaps(partitioning.KeyGroupRange{Start: 1, End: 3}), "overlaps from left")
	assert.True(t, kgr.Overlaps(partitioning.KeyGroupRange{Start: 3, End: 6}), "overlaps from right")
	assert.True(t, kgr.Overlaps(partitioning.KeyGroupRange{Start: 2, End: 5}), "exact match")

	// Non-overlapping cases
	assert.False(t, kgr.Overlaps(partitioning.KeyGroupRange{Start: 0, End: 2}), "no overlap left")
	assert.False(t, kgr.Overlaps(partitioning.KeyGroupRange{Start: 5, End: 7}), "no overlap right")
}

func TestKeyGroupRangeIncludesKeyGroup(t *testing.T) {
	kgr := partitioning.KeyGroupRange{Start: 2, End: 5}

	// Test included key groups
	assert.True(t, kgr.IncludesKeyGroup(partitioning.KeyGroup(2)), "includes start")
	assert.True(t, kgr.IncludesKeyGroup(partitioning.KeyGroup(3)), "includes middle")
	assert.True(t, kgr.IncludesKeyGroup(partitioning.KeyGroup(4)), "includes last")

	// Test excluded key groups
	assert.False(t, kgr.IncludesKeyGroup(partitioning.KeyGroup(1)), "excludes before start")
	assert.False(t, kgr.IncludesKeyGroup(partitioning.KeyGroup(5)), "excludes at end")
	assert.False(t, kgr.IncludesKeyGroup(partitioning.KeyGroup(6)), "excludes after end")
}
