package partitioning_test

import (
	"math/rand"
	"sync"
	"testing"

	"reduction.dev/reduction/partitioning"

	"github.com/stretchr/testify/assert"
)

func TestEventKeyPlacementBetweenRanges(t *testing.T) {
	// Given 256 key groups, 8 hosts, each host gets 32 key groups
	keySpace := partitioning.NewKeySpace(256, 8)

	// Make 8 key group ranges to track placements
	keyGroupRanges := make([]int, 8)

	for _, key := range randomKeys() {
		rangeIndex := keySpace.RangeIndex(key)
		keyGroupRanges[rangeIndex] += 1
	}

	// Keys evenly dispersed between key ranges
	assert.Equal(t, []int{1234, 1265, 1304, 1293, 1210, 1230, 1258, 1206}, keyGroupRanges)
}

func TestUnevenKeyPlacement_LastRangeShorter(t *testing.T) {
	// First 2 hosts will get 2 key groups, last will get only 1
	keySpace := partitioning.NewKeySpace(5, 3)
	keyGroupRanges := make([]int, 3)
	for _, key := range randomKeys() {
		rangeIndex := keySpace.RangeIndex(key)
		keyGroupRanges[rangeIndex] += 1
	}

	// Only 1/5 of keys (~2,000) placed in last range
	assert.Equal(t, []int{4015, 3937, 2048}, keyGroupRanges)

	// First host will get 3/5 key groups, last will get 2/5
	keySpace = partitioning.NewKeySpace(5, 2)
	keyGroupRanges = make([]int, 2)
	for _, key := range randomKeys() {
		rangeIndex := keySpace.RangeIndex(key)
		keyGroupRanges[rangeIndex] += 1
	}

	// 3/5 keys (~6,000) and 2/5 keys (~4,000)
	assert.Equal(t, []int{5987, 4013}, keyGroupRanges)
}

func TestUnevenKeyPlacement_TrailingRangesShort(t *testing.T) {
	// First host will get 2/4, last two will get 1/4 each
	keySpace := partitioning.NewKeySpace(4, 3)
	keyGroupRanges := make([]int, 3)
	for _, key := range randomKeys() {
		rangeIndex := keySpace.RangeIndex(key)
		keyGroupRanges[rangeIndex] += 1
	}

	// 1/2 keys (5,000) followed by 1/4th keys (~2,500)
	assert.Equal(t, []int{5054, 2459, 2487}, keyGroupRanges)
}

func TestListingAllKeyGroupRanges(t *testing.T) {
	// Given 4 key groups, 3 ranges
	keySpace := partitioning.NewKeySpace(4, 3)
	assert.Equal(t, []partitioning.KeyGroupRange{
		{0, 2},
		{2, 3},
		{3, 4},
	}, keySpace.KeyGroupRanges())

	// Given 256 key groups, 7 ranges
	keySpace = partitioning.NewKeySpace(256, 7)
	assert.Equal(t, []partitioning.KeyGroupRange{
		{0, 37},    // 37 KG
		{37, 74},   // 37 KG
		{74, 111},  // 37 KG
		{111, 148}, // 37 KG
		{148, 184}, // 36 KG
		{184, 220}, // 36 KG
		{220, 256}, // 36 KG
	}, keySpace.KeyGroupRanges())

	// Even placement of 16 key groups in 4 ranges
	keySpace = partitioning.NewKeySpace(16, 4)
	assert.Equal(t, []partitioning.KeyGroupRange{
		{0, 4},
		{4, 8},
		{8, 12},
		{12, 16},
	}, keySpace.KeyGroupRanges())
}

func TestAssigningOverlappingRangesToKeyGroupRanges(t *testing.T) {
	// Various overlapping
	assert.Equal(t,
		[][]int{
			{0, 1},
			{1, 2},
			{2},
		},
		partitioning.AssignRanges(
			[]partitioning.KeyGroupRange{ // to
				{0, 2},
				{2, 4},
				{4, 6},
			},
			[]partitioning.KeyGroupRange{ // from
				{0, 1},
				{1, 3},
				{3, 6},
			},
		),
	)

	// Assign many to one
	assert.Equal(t,
		[][]int{
			{0, 1, 2},
		},
		partitioning.AssignRanges(
			[]partitioning.KeyGroupRange{ // to
				{0, 6},
			},
			[]partitioning.KeyGroupRange{ // from
				{0, 1},
				{1, 3},
				{3, 6},
			},
		),
	)

	// Assign one to many
	assert.Equal(t,
		[][]int{
			{0}, {0}, {0},
		},
		partitioning.AssignRanges(
			[]partitioning.KeyGroupRange{ // to
				{0, 1},
				{1, 3},
				{3, 6},
			},
			[]partitioning.KeyGroupRange{ // from
				{0, 6},
			},
		),
	)

	// Assign many to first and only one to the last
	assert.Equal(t,
		[][]int{
			{0, 1, 2},
			{2},
		},
		partitioning.AssignRanges(
			[]partitioning.KeyGroupRange{ // to
				{0, 4},
				{4, 5},
			},
			[]partitioning.KeyGroupRange{ // from
				{0, 1},
				{1, 3},
				{3, 6},
			},
		),
	)
}

func TestAssignRangesWithEmptyFromValue(t *testing.T) {
	assert.Equal(t,
		[][]int{
			nil, // No assignments for each
			nil,
		},
		partitioning.AssignRanges(
			[]partitioning.KeyGroupRange{ // to
				{0, 1},
				{2, 3},
			},
			[]partitioning.KeyGroupRange{}, // from
		),
	)
}

func TestAssignRanges_NoOverlap(t *testing.T) {
	assert.Equal(t,
		[][]int{
			nil, // No assignments for each
			nil,
		},
		partitioning.AssignRanges(
			[]partitioning.KeyGroupRange{ // to
				{0, 1},
				{2, 3},
			},
			[]partitioning.KeyGroupRange{ // from
				{4, 5}, // No overlap with any 'to' range
			},
		),
	)
}

func TestKeyGroupConsistency(t *testing.T) {
	keySpace := partitioning.NewKeySpace(256, 8)
	key := []byte("my-key")

	// Same key should always map to same KeyGroup
	kg1 := keySpace.KeyGroup(key)
	kg2 := keySpace.KeyGroup(key)
	assert.Equal(t, kg1, kg2)
}

func TestKeyGroupBoundaries(t *testing.T) {
	keySpace := partitioning.NewKeySpace(256, 8)

	// Check distribution across available key groups
	seenGroups := make(map[partitioning.KeyGroup]bool)
	for _, key := range randomKeys() {
		kg := keySpace.KeyGroup(key)
		// KeyGroup should be within bounds
		assert.Less(t, uint16(kg), uint16(256))
		seenGroups[kg] = true
	}

	// With random distribution and 10k keys, we should see most groups used
	assert.Greater(t, len(seenGroups), 200)
}

// Use deterministic random generator and create keys to place.
var randomKeys = sync.OnceValue(func() [][]byte {
	r := rand.New(rand.NewSource(0))
	keys := make([][]byte, 10_000)
	for i := range keys {
		keys[i] = make([]byte, 16)
		r.Read(keys[i])
	}
	return keys
})
