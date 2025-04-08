package bloom

import (
	"encoding/binary"
	"io"

	"reduction.dev/reduction/dkv/fields"
	"reduction.dev/reduction/util/murmur"
)

type Filter struct {
	bitArray  []uint64
	size      uint32
	hashCount int
}

// NewFilter creates a BloomFilter with a size and number of hash functions
// to run.
func NewFilter(size uint32, hashes int) *Filter {
	// Calculate the number of uint64s needed to store `size` bits
	numUint64s := (size + 63) / 64

	return &Filter{
		bitArray:  make([]uint64, numUint64s),
		size:      size,
		hashCount: hashes,
	}
}

// Add inserts an element into the Bloom filter
func (bf *Filter) Add(data []byte) {
	for i := range bf.hashCount {
		hash := murmur.Hash(data, i)
		index := hash % bf.size
		bf.setBit(index)
	}
}

// MightHave checks if an element is possibly in the set
func (bf *Filter) MightHave(data []byte) bool {
	for i := range bf.hashCount {
		hash := murmur.Hash(data, i)
		index := hash % bf.size
		if !bf.getBit(index) {
			return false
		}
	}

	return true
}

func (bf *Filter) Encode(w io.Writer) int {
	n := fields.MustWriteUint32(w, bf.size)
	n += fields.MustWriteUint32(w, uint32(bf.hashCount))
	for _, num := range bf.bitArray {
		n += fields.MustWriteUint64(w, num)
	}
	return n
}

func Decode(r io.Reader) *Filter {
	size, err := fields.ReadUint32(r)
	if err != nil {
		panic(err)
	}
	hashes, err := fields.ReadUint32(r)
	if err != nil {
		panic(err)
	}

	bf := NewFilter(size, int(hashes))
	for i := range bf.bitArray {
		var num uint64
		if err := binary.Read(r, binary.LittleEndian, &num); err != nil {
			panic(err)
		}
		bf.bitArray[i] = num
	}

	return bf
}

// setBit sets the bit at position `pos` in the bit array
func (bf *Filter) setBit(pos uint32) {
	index := pos / 64                 // Determine which uint64 element to use
	bitPos := pos % 64                // Determine the bit position within that uint64
	bf.bitArray[index] |= 1 << bitPos // Set the bit
}

// getBit returns true if the bit at position `pos` is set, false otherwise
func (bf *Filter) getBit(pos uint32) bool {
	index := pos / 64  // Determine which uint64 element to use
	bitPos := pos % 64 // Determine the bit position within that uint64
	return (bf.bitArray[index] & (1 << bitPos)) != 0
}
