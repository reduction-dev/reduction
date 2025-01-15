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
