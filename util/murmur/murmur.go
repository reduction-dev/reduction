package murmur

// Reference C++ implementation: https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp

import (
	"math/bits"
)

func Hash(data []byte, seed int) uint32 {
	h1 := uint32(seed)
	inputLen := uint32(len(data))

	const c1 uint32 = 0xcc9e2d51
	const c2 uint32 = 0x1b873593

	// Body - Process most of the bytes 4 at a time

	for len(data) >= 4 {
		// Convert next 4 bytes into a uint32
		k1 := uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16 | uint32(data[3])<<24

		k1 *= c1
		k1 = bits.RotateLeft32(k1, 15)
		k1 *= c2

		h1 ^= k1
		h1 = bits.RotateLeft32(h1, 13)
		h1 = h1*5 + 0xe6546b64

		// Remove the 4 processed bytes
		data = data[4:]
	}

	// Tail - Process any remaining data that didn't fit in 4 bytes chunks

	k1 := uint32(0)
	switch len(data) & 3 {
	case 3:
		k1 ^= uint32(data[2]) << 16
		fallthrough
	case 2:
		k1 ^= uint32(data[1]) << 8
		fallthrough
	case 1:
		k1 ^= uint32(data[0])
	}

	k1 *= c1
	k1 = bits.RotateLeft32(k1, 15)
	k1 *= c2
	h1 ^= k1

	// Finalization

	h1 ^= inputLen
	h1 ^= h1 >> 16
	h1 *= 0x85ebca6b
	h1 ^= h1 >> 13
	h1 *= 0xc2b2ae35
	h1 ^= h1 >> 16

	return h1
}
