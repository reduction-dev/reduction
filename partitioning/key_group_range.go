package partitioning

import (
	"fmt"

	"reduction.dev/reduction/proto/snapshotpb"
)

// KeyGroupRange is a range of KeyGroups where start is inclusive and end is exclusive.
type KeyGroupRange struct {
	Start int
	End   int
}

func KeyGroupRangeFromProto(msg *snapshotpb.KeyGroupRange) KeyGroupRange {
	return KeyGroupRange{
		Start: int(msg.Start),
		End:   int(msg.End),
	}
}

// KeyGroupRangeFromBytes creates a KeyGroupRange from byte slices. In this case
// the `end` slice is _inclusive_ but this function yields a KeyGroupRange with
// an exclusive end that would contain the given keys.
func KeyGroupRangeFromBytes(start, end []byte) KeyGroupRange {
	return KeyGroupRange{
		Start: int(KeyGroupFromBytes(start)),
		End:   int(KeyGroupFromBytes(end)) + 1,
	}
}

func (r KeyGroupRange) Overlaps(other KeyGroupRange) bool {
	return other.Start < r.End && other.End > r.Start
}

func (r KeyGroupRange) Contains(other KeyGroupRange) bool {
	return other.Start >= r.Start && other.End <= r.End
}

func (r KeyGroupRange) String() string {
	return fmt.Sprintf("[%d, %d]", r.Start, r.End)
}

func (r KeyGroupRange) Size() int {
	return r.End - r.Start
}

// KeyGroups returns a slices of all key groups in the range.
func (r KeyGroupRange) KeyGroups() []KeyGroup {
	keyGroups := make([]KeyGroup, r.Size())
	for i := 0; i < r.Size(); i++ {
		keyGroups[i] = KeyGroup(r.Start + i)
	}
	return keyGroups
}

// IndexOf returns the relative index of a key group in the range.
func (r KeyGroupRange) IndexOf(kg KeyGroup) int {
	return int(kg) - r.Start
}

// IncludesKeyGroup returns true if the given KeyGroup falls within this range.
// Start is inclusive, End is exclusive.
func (r KeyGroupRange) IncludesKeyGroup(kg KeyGroup) bool {
	kgInt := int(kg)
	return kgInt >= r.Start && kgInt < r.End
}
