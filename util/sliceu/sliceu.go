package sliceu

import (
	"math/rand"
	"slices"
)

func Last[T any](slice []T) T {
	return slice[len(slice)-1]
}

func Map[T, U any](s []T, f func(T) U) []U {
	result := make([]U, len(s))
	for i, v := range s {
		result[i] = f(v)
	}
	return result
}

func Partition[T any](slice []T, groupCount int) [][]T {
	if groupCount < 1 {
		panic("Partition groupCount must be at least 1")
	}
	groups := make([][]T, groupCount)
	groupIndex := 0
	maxGroupIndex := groupCount - 1
	for _, el := range slice {
		groups[groupIndex] = append(groups[groupIndex], el)
		if groupIndex < maxGroupIndex {
			groupIndex++
		} else {
			groupIndex = 0
		}
	}

	return groups
}

// Return a new slice with only the items indicated by idxList.
func Pick[T any](s []T, idxList []int) []T {
	ret := make([]T, 0, len(idxList))
	for _, idx := range idxList {
		ret = append(ret, s[idx])
	}
	return ret
}

// Return true if every item meets the condition. If the input slice is empty,
// this function returns true.
func Every[T any](s []T, predicate func(T) bool) bool {
	for _, item := range s {
		if !predicate(item) {
			return false
		}
	}
	return true
}

func Without[T comparable](s []T, item T) []T {
	index := slices.Index(s, item)
	if index == -1 {
		return s // Item not found, return the original slice
	}
	return slices.Delete(s, index, index+1)
}

// Sample items from a slice. The returned slice may contain duplicates.
func Sample[T any](slice []T, n int) []T {
	if len(slice) == 0 || n <= 0 {
		return nil
	}

	result := make([]T, n)
	for i := range n {
		result[i] = slice[rand.Intn(len(slice))]
	}

	return result
}

// SampleOne returns one item at random from the given slice.
func SampleOne[T any](slice []T) T {
	if len(slice) == 0 {
		panic("tried to sample empty slice")
	}
	return slice[rand.Intn(len(slice))]
}

// SearchUnique is like the slices.BinarySearchFunc except that it assumes there
// are no duplicate elements in the list and stops when it finds a result rather
// than looking for the earliest found instance.
func SearchUnique[S ~[]E, E, T any](x S, target T, cmp func(E, T) int) (int, bool) {
	low, high := 0, len(x)
	for low < high {
		i := int(uint(low+high) >> 1) // avoid overflow when computing h
		cmpValue := cmp(x[i], target)

		// Found
		if cmpValue == 0 {
			return i, true
		}

		// Continue search
		if cmpValue < 0 {
			low = i + 1
		} else {
			high = i - 1
		}
	}

	return 0, false
}

// Unique returns a list of the unique elements in the given slice.
func UniqueFunc[T, K comparable](in []T, fn func(el T) K) []K {
	m := make(map[K]struct{}, len(in))
	for _, el := range in {
		m[fn(el)] = struct{}{}
	}

	keys := make([]K, len(m))
	i := 0
	for k := range m {
		keys[i] = k
		i++
	}

	return keys
}

func KeyMap[T comparable](sl []T) map[T]struct{} {
	out := make(map[T]struct{}, len(sl))
	for _, v := range sl {
		out[v] = struct{}{}
	}
	return out
}
