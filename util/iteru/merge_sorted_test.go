package iteru

import (
	"iter"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMergeSort(t *testing.T) {
	intCmp := func(a, b int) int { return a - b }

	tests := []struct {
		name     string
		inputs   [][]int
		expected []int
	}{
		{
			name:     "empty input",
			inputs:   nil,
			expected: nil,
		},
		{
			name:     "single iterator",
			inputs:   [][]int{{1, 2, 3}},
			expected: []int{1, 2, 3},
		},
		{
			name:     "multiple iterators",
			inputs:   [][]int{{1, 4}, {2, 3}, {5}},
			expected: []int{1, 2, 3, 4, 5},
		},
		{
			name:     "multiple, uneven iterators",
			inputs:   [][]int{{1}, {2, 4, 6, 8}, {3, 5, 7}},
			expected: []int{1, 2, 3, 4, 5, 6, 7, 8},
		},
		{
			name:     "with duplicates",
			inputs:   [][]int{{1, 2}, {1, 3, 3}, {2}},
			expected: []int{1, 1, 2, 2, 3, 3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Convert slices to iterators
			iters := make([]iter.Seq[int], len(tt.inputs))
			for i, input := range tt.inputs {
				iters[i] = slices.Values(input)
			}

			result := slices.Collect(MergeSorted(iters, intCmp))
			assert.Equal(t, tt.expected, result)
		})
	}
}
