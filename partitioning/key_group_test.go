package partitioning_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"reduction.dev/reduction/partitioning"
)

func TestKeyGroupRoundTrip(t *testing.T) {
	kg := partitioning.KeyGroup(42)

	// Write the KeyGroup to a buffer
	buf := make([]byte, 2)
	kg.PutBytes(buf)

	// Read it back
	result := partitioning.KeyGroupFromBytes(buf)
	assert.Equal(t, kg, result)
}

func TestKeyGroupFromBytesInvalidLength(t *testing.T) {
	assert.Panics(t, func() {
		partitioning.KeyGroupFromBytes([]byte{1}) // Only 1 byte
	})

	assert.Panics(t, func() {
		partitioning.KeyGroupFromBytes([]byte{1, 2, 3}) // 3 bytes
	})
}

func TestKeyGroupRangeContains(t *testing.T) {
	kgr := partitioning.KeyGroupRange{Start: 2, End: 5}

	// Test contained ranges
	assert.True(t, kgr.Contains(partitioning.KeyGroupRange{Start: 2, End: 5}), "contains itself")
	assert.True(t, kgr.Contains(partitioning.KeyGroupRange{Start: 2, End: 4}), "contains subset starting at start")
	assert.True(t, kgr.Contains(partitioning.KeyGroupRange{Start: 3, End: 5}), "contains subset ending at end")
	assert.True(t, kgr.Contains(partitioning.KeyGroupRange{Start: 3, End: 4}), "contains inner subset")

	// Test non-contained ranges
	assert.False(t, kgr.Contains(partitioning.KeyGroupRange{Start: 1, End: 5}), "does not contain range extending before start")
	assert.False(t, kgr.Contains(partitioning.KeyGroupRange{Start: 2, End: 6}), "does not contain range extending past end")
	assert.False(t, kgr.Contains(partitioning.KeyGroupRange{Start: 1, End: 6}), "does not contain larger range")
	assert.False(t, kgr.Contains(partitioning.KeyGroupRange{Start: 0, End: 2}), "does not contain non-overlapping range before")
	assert.False(t, kgr.Contains(partitioning.KeyGroupRange{Start: 5, End: 7}), "does not contain non-overlapping range after")
}
