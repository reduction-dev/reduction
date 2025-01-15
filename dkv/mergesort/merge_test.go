package mergesort_test

import (
	"cmp"
	"iter"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"reduction.dev/reduction/dkv/mergesort"
)

func TestMerge_SequentialValues(t *testing.T) {
	input := [][]int{
		{1, 2, 3, 4, 5},
		{6, 7, 8},
		{9, 10, 11, 12},
		{},
	}

	inputIterators := make([]iter.Seq[int], len(input))
	for i, slice := range input {
		inputIterators[i] = slices.Values(slice)
	}

	it := mergesort.Merge(
		inputIterators,
		func(a, b int) int { return cmp.Compare(a, b) },
		func(a, b int) int { return a })

	assert.Equal(t,
		[]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
		slices.Collect(it))
}

func TestMerge_SupersededValues(t *testing.T) {
	type kv struct {
		k int
		v int
	}
	input := [][]kv{
		{{1, 6}, {1, 5}},
		{{1, 4}, {1, 3}, {2, 3}, {2, 2}},
		{{1, 2}, {1, 1}, {2, 1}},
	}

	inputIterators := make([]iter.Seq[kv], len(input))
	for i, slice := range input {
		inputIterators[i] = slices.Values(slice)
	}

	it := mergesort.Merge(
		inputIterators,
		func(a, b kv) int { return cmp.Compare(a.k, b.k) },
		func(a, b kv) kv {
			if a.v > b.v {
				return a
			}
			return b
		},
	)

	assert.Equal(t,
		[]kv{{1, 6}, {2, 3}},
		slices.Collect(it))
}
