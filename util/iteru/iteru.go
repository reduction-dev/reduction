package iteru

import (
	"iter"
)

// Every returns true if every item in the sequence is true.
func Every(seq iter.Seq[bool]) bool {
	for v := range seq {
		if !v {
			return v
		}
	}
	return true
}

// Times creates a sequence of int from 0 to n.
func Times(n int) iter.Seq[int] {
	return func(yield func(int) bool) {
		for i := 0; i < n; i++ {
			if !yield(i) {
				return
			}
		}
	}
}

// Reduce combines the values in seq using f.
// For each value v in seq, it updates sum = f(sum, v)
// and then returns the final sum.
// For example, if iterating over seq yields v1, v2, v3,
// Reduce returns f(f(f(sum, v1), v2), v3).
func Reduce[Sum, V any](f func(Sum, V) Sum, sum Sum, seq iter.Seq[V]) Sum {
	for v := range seq {
		sum = f(sum, v)
	}
	return sum
}

// Reduce2 combines the values in seq using f.
// For each pair k, v in seq, it updates sum = f(sum, k, v)
// and then returns the final sum.
// For example, if iterating over seq yields (k1, v1), (k2, v2), (k3, v3)
// Reduce returns f(f(f(sum, k1, v1), k2, v2), k3, v3).
func Reduce2[Sum, K, V any](f func(Sum, K, V) Sum, sum Sum, seq iter.Seq2[K, V]) Sum {
	for k, v := range seq {
		sum = f(sum, k, v)
	}
	return sum
}

// Collect both iterator values into 2 separate slices.
func Collect2[T1, T2 any](it iter.Seq2[T1, T2]) ([]T1, []T2) {
	var values1 []T1
	var values2 []T2
	for v1, v2 := range it {
		values1 = append(values1, v1)
		values2 = append(values2, v2)
	}
	return values1, values2
}

// Combine a list of iterators to return one that iterates through all sequences
// in order.
func Concat[T any](seqs ...iter.Seq[T]) iter.Seq[T] {
	return func(yield func(T) bool) {
		for _, seq := range seqs {
			for el := range seq {
				if !yield(el) {
					return
				}
			}
		}
	}
}

// Concat2 returns an iterator over the concatenation of the sequences.
func Concat2[K, V any](seqs ...iter.Seq2[K, V]) iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		for _, seq := range seqs {
			for k, v := range seq {
				if !yield(k, v) {
					return
				}
			}
		}
	}
}

// Return the minimum value in the iterator according to the cmp function.
// Like MinFunc on slices, this panics if the sequence is empty.
func MinFunc[T any](it iter.Seq[T], cmp func(a, b T) int) T {
	var min *T
	for cur := range it {
		if min == nil || cmp(cur, *min) < 0 {
			min = &cur
		}
	}
	if min == nil {
		panic("iteru.MinFunc: empty sequence")
	}
	return *min
}
