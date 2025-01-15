package ds

import (
	"iter"
	"sync"
)

// LockingMap is a wrapper around a map that provides a mutex lock.
type LockingMap[K comparable, V any] struct {
	m  map[K]V
	mu sync.RWMutex
}

func NewLockingMap[K comparable, V any]() *LockingMap[K, V] {
	return &LockingMap[K, V]{
		m:  make(map[K]V),
		mu: sync.RWMutex{},
	}
}

func (m *LockingMap[K, V]) Get(key K) (V, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	value, ok := m.m[key]
	return value, ok
}

func (m *LockingMap[K, V]) Put(key K, value V) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.m[key] = value
}

func (m *LockingMap[K, V]) Delete(key K) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.m, key)
}

func (m *LockingMap[K, V]) Size() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return len(m.m)
}

func (m *LockingMap[K, V]) All() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		m.mu.RLock()
		defer m.mu.RUnlock()

		for k, v := range m.m {
			m.mu.RUnlock() // Unlock while yielding
			if !yield(k, v) {
				// Must re-lock on any exit of the loop, anticipating the deferred unlock.
				m.mu.RLock()
				return
			}
			m.mu.RLock()
		}
	}
}
