package partitioning

import (
	"encoding/binary"
)

// KeyGroup represents a group number that a key belongs to.
type KeyGroup uint16

// PutBytes writes the KeyGroup as two bytes in big-endian order to the provided slice.
func (kg KeyGroup) PutBytes(b []byte) {
	binary.BigEndian.PutUint16(b, uint16(kg))
}

// KeyGroupFromBytes constructs a KeyGroup from two bytes in big-endian order.
// This will panic if the input slice length is not exactly 2 bytes.
func KeyGroupFromBytes(b []byte) KeyGroup {
	if len(b) != 2 {
		panic("KeyGroupFromBytes requires exactly 2 bytes")
	}
	return KeyGroup(binary.BigEndian.Uint16(b))
}
