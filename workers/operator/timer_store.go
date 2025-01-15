package operator

import (
	"bytes"
	"encoding/binary"
	"time"

	"reduction.dev/reduction/dkv"
	"reduction.dev/reduction/partitioning"
	"reduction.dev/reduction/util/binu"
	"reduction.dev/reduction/util/ds"
)

// TimerStore persists timers in DKV. It manages a write-through cache to
// minimize scan queries.
type TimerStore struct {
	// Caches mapped by key group
	priorityQueue *ds.PartitionedPriorityQueue[[]byte]

	// KeySpace used to get the key group for keys
	keySpace *partitioning.KeySpace
}

type Timer struct {
	Timestamp time.Time
	Key       []byte
	dbKey     []byte
	kg        partitioning.KeyGroup
}

func NewTimerStore(db *dkv.DB, keySpace *partitioning.KeySpace, keyGroupRange partitioning.KeyGroupRange, maxCacheSize uint64) *TimerStore {
	// Make partitions for each key group
	partitions := make([]ds.QueuePartition[[]byte], keyGroupRange.Size())
	for i, kg := range keyGroupRange.KeyGroups() {
		partitions[i] = NewKeyGroupPriorityQueue(db, partitioning.KeyGroup(kg), maxCacheSize/uint64(keyGroupRange.Size()))
	}

	compare := func(a, b []byte) int {
		// Compare just the timestamps between partitions, not the key groups.
		return bytes.Compare(a[3:11], b[3:11])
	}

	// The partition index adjusts the key group number to be within the list of partitions.
	getPartitionIndex := func(key []byte) int {
		kg := partitioning.KeyGroupFromBytes(key[0:2])
		return keyGroupRange.IndexOf(kg)
	}
	priorityQueue := ds.NewPartitionedPriorityQueue(partitions, compare, getPartitionIndex)

	return &TimerStore{
		priorityQueue: priorityQueue,
		keySpace:      keySpace,
	}
}

func (s *TimerStore) Put(subjectKey []byte, t time.Time) {
	_, key := s.encodeTimerKey(subjectKey, t)
	s.priorityQueue.Push(key)
}

func (s *TimerStore) GetEarliest() (timer Timer, ok bool) {
	b, ok := s.priorityQueue.Peek()
	if !ok {
		return Timer{}, false
	}

	return s.timerFromBytes(b), true
}

func (s *TimerStore) Pop() (timer Timer, ok bool) {
	b, ok := s.priorityQueue.Pop()
	if !ok {
		return Timer{}, false
	}

	return s.timerFromBytes(b), true
}

func (s *TimerStore) Delete(timer Timer) {
	s.priorityQueue.Delete(timer.dbKey)
}

// Create a key like <key-group><schema><timestamp><subject-key>
// Uses schema byte 0x01 to avoid conflicts with KeyedStateStore (0x00)
// encodeTimerKey creates a key with the following structure:
// <2 bytes key-group><1 byte schema><8 bytes timestamp><subject-key>
func (s *TimerStore) encodeTimerKey(subjectKey []byte, t time.Time) (kg partitioning.KeyGroup, key []byte) {
	// 2 key group bytes + 1 schema byte + 8 timestamp bytes + subject key
	encodedKey := make([]byte, 2+1+8+len(subjectKey))

	kg = s.keySpace.KeyGroup(subjectKey)

	kg.PutBytes(encodedKey[0:2])
	encodedKey[2] = 0x01 // Write schema byte
	binu.PutTimeBytes(encodedKey[3:11], t)
	copy(encodedKey[11:], subjectKey)

	return kg, encodedKey
}

func (s *TimerStore) timerFromBytes(b []byte) Timer {
	kg := partitioning.KeyGroupFromBytes(b[0:2])
	timestamp := binu.TimeFromBytes(b[3:11]) // Skip subjectKey group and schema bytes
	subjectKey := b[11:]                     // Everything after timestamp
	return Timer{
		Key:       subjectKey,
		Timestamp: timestamp,
		dbKey:     b,
		kg:        kg,
	}
}

// A KeyGroupPriorityQueue is a priority queue for a single key group. It uses a
// write-through cache to minimize DKV queries and lazily load data from the
// db.
type KeyGroupPriorityQueue struct {
	cache          *ds.SortedCache
	keyGroup       partitioning.KeyGroup
	db             *dkv.DB
	allDataInCache bool
	index          int
}

func NewKeyGroupPriorityQueue(db *dkv.DB, keyGroup partitioning.KeyGroup, cacheSize uint64) *KeyGroupPriorityQueue {
	return &KeyGroupPriorityQueue{
		cache:    ds.NewSortedCache(cacheSize),
		keyGroup: keyGroup,
		db:       db,
	}
}

func (pq *KeyGroupPriorityQueue) Peek() ([]byte, bool) {
	pq.loadFromDB()
	return pq.cache.Peek()
}

func (pq *KeyGroupPriorityQueue) Pop() ([]byte, bool) {
	pq.loadFromDB()
	min, ok := pq.cache.Pop()
	if ok {
		pq.db.Delete(min)
	}
	return min, ok
}

func (pq *KeyGroupPriorityQueue) Push(data []byte) {
	pq.loadFromDB()
	pq.cache.Push(data)

	// If pushing the item exceeded the cache capacity, evict items until we're back under the limit
	for pq.cache.IsFull() && !pq.cache.IsEmpty() {
		pq.cache.PopLast()
		pq.allDataInCache = false // evicted item is now only in the DB
	}

	pq.db.Put(data, nil) // write-through cache to db
}

func (pq *KeyGroupPriorityQueue) AssignIndex(i int) {
	pq.index = i
}

func (pq *KeyGroupPriorityQueue) Index() int {
	return pq.index
}

func (pq *KeyGroupPriorityQueue) IsEmpty() bool {
	pq.loadFromDB()
	return pq.cache.IsEmpty()
}

// Delete removes an element from the queue.
func (pq *KeyGroupPriorityQueue) Delete(data []byte) {
	pq.loadFromDB()
	pq.cache.Delete(data)
	pq.db.Delete(data)
}

// Load all of the given KeyGroup's data into the cache.
func (pq *KeyGroupPriorityQueue) loadFromDB() {
	if !pq.cache.IsEmpty() || pq.allDataInCache {
		return
	}

	prefix := make([]byte, 3) // 2 key group bytes + 1 schema byte
	binary.BigEndian.PutUint16(prefix[0:2], uint16(pq.keyGroup))
	prefix[2] = 0x01 // Schema byte

	var err error
	for entry := range pq.db.ScanPrefix(prefix, &err) {
		pq.cache.Push(entry.Key())
		if pq.cache.IsFull() {
			break
		}
	}
	if err != nil {
		panic(err)
	}
	pq.allDataInCache = true
}

var _ ds.QueuePartition[[]byte] = &KeyGroupPriorityQueue{}
