package partitioning

import (
	"fmt"
	"math"

	"reduction.dev/reduction/util/murmur"
)

// The total set of available keyRanges where keys can be placed.
type KeySpace struct {
	// The number of KeyGroups. Each KeyGroup is assigned to a KeyGroupRange.
	keyGroupCount uint32
	// The number of KeyGroupRanges holding KeyGroups.
	rangeCount uint32
	// A precomputed lookup table to find a KeyGroupRange given a KeyGroup in 0(1)
	// time.
	rangeLookup []uint16
	// Precomputed list of key group ranges.
	keyGroupRanges []KeyGroupRange
}

func NewKeySpace(keyGroupCount int, keyGroupRangeCount int) *KeySpace {
	if keyGroupCount < 1 || keyGroupCount > math.MaxUint16 {
		panic(fmt.Sprintf("keyGroupCount must be greater than 0 and less than or equal to %d", math.MaxInt16))
	}
	if keyGroupRangeCount < 1 {
		panic("keyGroupRangeCount must be greater than 0")
	}

	// Go through each range and create a lookup table where the KeyGroup number
	// yields the index of a KeyGroupRange.
	rangeLookup := make([]uint16, keyGroupCount)
	kgRanges := keyGroupRanges(keyGroupCount, keyGroupRangeCount)
	for i, r := range kgRanges {
		for j := r.Start; j < r.End; j++ {
			rangeLookup[j] = uint16(i)
		}
	}

	return &KeySpace{
		keyGroupCount:  uint32(keyGroupCount),
		rangeCount:     uint32(keyGroupRangeCount),
		rangeLookup:    rangeLookup,
		keyGroupRanges: kgRanges,
	}
}

// KeyGroup returns the KeyGroup for a given key.
func (s *KeySpace) KeyGroup(key []byte) KeyGroup {
	hash := murmur.Hash(key, 0)
	return KeyGroup(hash % s.keyGroupCount)
}

// RangeIndex returns an index for the KeyGroupRange where a key should be
// placed.
func (s *KeySpace) RangeIndex(key []byte) int {
	kg := uint16(s.KeyGroup(key))
	return int(s.rangeLookup[kg])
}

func (s *KeySpace) KeyGroupRanges() []KeyGroupRange {
	return s.keyGroupRanges
}

// Compute a list of key group ranges. These ranges are sequential without
// any gaps or overlapping values.
func keyGroupRanges(keyGroupCount, rangeCount int) []KeyGroupRange {
	// The number of ranges at the beginning key group ranges that will have +1
	// more key groups. The later ranges will have max - 1 key groups.
	biggerRangeCount := keyGroupCount % rangeCount

	// The minimum number of key groups to place in a range.
	minKGInRange := keyGroupCount / rangeCount

	kgIndex := 0
	ranges := make([]KeyGroupRange, rangeCount)
	for i := range ranges {
		end := kgIndex + minKGInRange
		if i < biggerRangeCount {
			end++
		}
		ranges[i] = KeyGroupRange{
			Start: kgIndex,
			End:   end,
		}
		kgIndex = end
	}

	return ranges
}

// Return a slice where each slice index corresponds to a an item in `to` where
// the value is a list of KegGroup indices assigned from the `from`
// KeyGroupRanges.
func AssignRanges(to []KeyGroupRange, from []KeyGroupRange) [][]int {
	// The assignments to return
	assignments := make([][]int, len(to))
	for i := range assignments {
		assignments[i] = []int{}
	}
	if len(from) == 0 {
		return assignments
	}

	toIdx := 0
	fromIdx := 0
	for toIdx < len(to) {
		for fromIdx < len(from) && to[toIdx].Overlaps(from[fromIdx]) {
			assignments[toIdx] = append(assignments[toIdx], fromIdx)
			fromIdx++
		}
		fromIdx--
		toIdx++
	}
	return assignments
}
