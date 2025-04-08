package bloom_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"reduction.dev/reduction/dkv/bloom"
)

func TestBloomFilter(t *testing.T) {
	bitArraySize := uint32(100_000) // Size of the Bloom filter bit array
	numHashes := 7                  // Number of hash functions
	numKeysToAdd := 5_000           // Number of keys to add
	numKeysToTest := 1_000          // Number of keys to test for false positives
	falsePositiveThreshold := 0.01  // Allowable false positive rate (1%)

	filter := bloom.NewFilter(bitArraySize, numHashes)

	// Add random keys with known prefix to filter.
	addedKeys := make([][]byte, numKeysToAdd)
	for i := range addedKeys {
		key := fmt.Appendf(nil, "added-%d", i)
		addedKeys[i] = key
		filter.Add(key)
	}

	// Check that _all_ added keys are reported maybe in the filter.
	for _, key := range addedKeys {
		assert.True(t, filter.MightHave(key), "filter might have added key")
	}

	// Find the false positive rate for keys that were not added
	falsePositiveCount := 0
	for i := range numKeysToTest {
		key := fmt.Appendf(nil, "not-added-%d", i)
		if filter.MightHave(key) {
			falsePositiveCount++
		}
	}
	falsePositiveRate := float64(falsePositiveCount) / float64(numKeysToTest)

	assert.Less(t, falsePositiveRate, falsePositiveThreshold)
}
