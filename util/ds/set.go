package ds

import (
	"fmt"
	"iter"
	"maps"
	"slices"
)

// Set is a set data structure that maintains insertion order. Without is
// an O(N) operation.
type Set[T comparable] struct {
	m map[T]struct{}
	l []T
}

func NewSet[T comparable](capacity int) *Set[T] {
	return &Set[T]{
		m: make(map[T]struct{}, capacity),
		l: make([]T, 0, capacity),
	}
}

func SetOf[T comparable](vs ...T) *Set[T] {
	s := NewSet[T](len(vs))
	for _, v := range vs {
		s.Add(v)
	}
	return s
}

func (s *Set[T]) Add(v ...T) {
	for _, v := range v {
		if !s.Has(v) {
			s.m[v] = struct{}{}
			s.l = append(s.l, v)
		}
	}
}

func (s *Set[T]) Added(v ...T) *Set[T] {
	next := s.clone()
	next.Add(v...)
	return next
}

func (s *Set[T]) Without(vs ...T) *Set[T] {
	next := s.clone()
	for _, v := range vs {
		delete(next.m, v)
	}
	next.l = slices.DeleteFunc(next.l, func(el T) bool {
		return slices.Contains(vs, el)
	})
	return next
}

// The number of items in the set.
func (s *Set[T]) Size() int {
	if s == nil {
		return 0
	}
	return len(s.l)
}

func (s *Set[T]) All() iter.Seq[T] {
	if s == nil {
		return func(yield func(T) bool) {}
	}

	return func(yield func(T) bool) {
		for _, item := range s.l {
			if !yield(item) {
				return
			}
		}
	}
}

func (s *Set[T]) Slice() []T {
	return s.l
}

func (s *Set[T]) Has(v T) bool {
	_, ok := s.m[v]
	return ok
}

func (s *Set[T]) Diff(s2 *Set[T]) *Set[T] {
	diff := NewSet[T](s.Size())
	for e := range s.All() {
		if !s2.Has(e) {
			diff.Add(e)
		}
	}
	return diff
}

func (s *Set[T]) clone() *Set[T] {
	return &Set[T]{
		m: maps.Clone(s.m),
		l: slices.Clone(s.l),
	}
}

func (s *Set[T]) String() string {
	return fmt.Sprintf("%v", s.l)
}
