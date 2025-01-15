package sliceu_test

import (
	"testing"

	"reduction.dev/reduction/util/sliceu"

	"github.com/stretchr/testify/assert"
)

func TestPartition(t *testing.T) {
	slice := []int{1, 2, 3, 4, 5}
	groups := sliceu.Partition(slice, 2)

	assert.Len(t, groups, 2)
	assert.Equal(t, groups[0], []int{1, 3, 5})
	assert.Equal(t, groups[1], []int{2, 4})
}

func TestWithout(t *testing.T) {
	slice := []int{1, 2, 3}
	assert.Equal(t, []int{1, 2, 3}, sliceu.Without(slice, 4), "unchanged when no item found")

	slice = []int{1, 2, 3}
	assert.Equal(t, []int{1, 3}, sliceu.Without(slice, 2))

	slice = []int{1, 2}
	assert.Equal(t, []int{1}, sliceu.Without(slice, 2))

	slice = []int{1}
	assert.Equal(t, []int{}, sliceu.Without(slice, 1))
}
