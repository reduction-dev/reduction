package ds_test

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"reduction.dev/reduction/util/ds"
)

func TestSet_IterateInOrder(t *testing.T) {
	s := ds.NewSet[int](0)
	s.Add(1, 2, 3)

	inOrderElements := slices.Collect(s.All())
	assert.Equal(t, []int{1, 2, 3}, inOrderElements)
}

func TestSet_OnlyUniqueElements(t *testing.T) {
	s := ds.NewSet[int](0)
	s.Add(1, 2, 2, 3, 3, 3)

	inOrderElements := slices.Collect(s.All())
	assert.Equal(t, []int{1, 2, 3}, inOrderElements)
	assert.Equal(t, 3, s.Size())
}

func TestSet_Diff(t *testing.T) {
	s1 := ds.SetOf(1, 2, 3, 4)
	s2 := ds.SetOf(2, 4)
	s3 := s1.Diff(s2)

	assert.Equal(t, []int{1, 3}, slices.Collect(s3.All()))
}

func TestSet_Without(t *testing.T) {
	set := ds.SetOf(1, 2, 3)
	set = set.Without(2)

	assert.Equal(t, []int{1, 3}, slices.Collect(set.All()))
}
