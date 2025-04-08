package operator_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"reduction.dev/reduction/dkv"
	"reduction.dev/reduction/dkv/storage"
	"reduction.dev/reduction/partitioning"
	"reduction.dev/reduction/util/binu"
	"reduction.dev/reduction/util/iteru"
	"reduction.dev/reduction/util/queues"
	"reduction.dev/reduction/util/size"
	"reduction.dev/reduction/workers/operator"
)

func TestTimerStore_OwningFullKeySpace(t *testing.T) {
	db := dkv.Open(dkv.DBOptions{
		FileSystem: storage.NewMemoryFilesystem(),
	}, nil)
	keySpace := partitioning.NewKeySpace(256, 8)
	store := operator.NewTimerStore(db, keySpace, partitioning.KeyGroupRange{Start: 0, End: 256}, size.MB)

	t1 := time.Unix(1, 0)
	t2 := time.Unix(2, 0)
	t3 := time.Unix(3, 0)

	// Should return nothing when empty
	timer, ok := store.GetEarliest()
	assert.False(t, ok)

	// Add timers out of order
	store.Put([]byte("key2"), t2)
	store.Put([]byte("key3"), t3)
	store.Put([]byte("key1"), t1)

	// Should return earliest timer
	timer, ok = store.GetEarliest()
	assert.True(t, ok)
	assert.Equal(t, t1, timer.Timestamp)
	assert.Equal(t, []byte("key1"), timer.Key)

	// Delete earliest timer
	store.Delete(timer)

	// Should return next timer
	timer, ok = store.GetEarliest()
	assert.True(t, ok)
	assert.Equal(t, t2, timer.Timestamp)
	assert.Equal(t, []byte("key2"), timer.Key)

	// Create a new store instance to verify persistence
	store2 := operator.NewTimerStore(db, keySpace, partitioning.KeyGroupRange{Start: 0, End: 256}, size.MB)
	timer, ok = store2.GetEarliest()
	assert.True(t, ok)
	assert.Equal(t, t2, timer.Timestamp)
	assert.Equal(t, []byte("key2"), timer.Key)
}

func TestTimerStore_OwningHalfKeySpace(t *testing.T) {
	db := dkv.Open(dkv.DBOptions{
		FileSystem: storage.NewMemoryFilesystem(),
	}, nil)

	// Setup a key space with 2 key group ranges.
	keySpace := partitioning.NewKeySpace(256, 2)
	kgrs := keySpace.KeyGroupRanges()
	assert.Len(t, kgrs, 2)

	// Create a store for each key group range
	stores := []*operator.TimerStore{
		operator.NewTimerStore(db, keySpace, kgrs[0], size.MB),
		operator.NewTimerStore(db, keySpace, kgrs[1], size.MB),
	}

	// Route 10 keys and timers the stores
	for i := range iteru.Times(10) {
		subjectKey := binu.IntBytes(uint64(i))
		rangeIdx := keySpace.RangeIndex(subjectKey)
		stores[rangeIdx].Put(subjectKey, time.Unix(int64(i), 0))
	}

	// Check store 1 keys
	var store1Keys [][]byte
	for _, timer := range queues.Drain(stores[0]) {
		store1Keys = append(store1Keys, timer.Key)
	}
	assert.Equal(t, binu.IntBytesList(1, 2, 3, 4, 8, 9), store1Keys)

	// Check store 2 keys
	var store2Keys [][]byte
	for _, timer := range queues.Drain(stores[1]) {
		store2Keys = append(store2Keys, timer.Key)
	}
	assert.Equal(t, binu.IntBytesList(0, 5, 6, 7), store2Keys)
}

func TestTimerStore_ExceedingCacheSize(t *testing.T) {
	db := dkv.Open(dkv.DBOptions{
		FileSystem: storage.NewMemoryFilesystem(),
	}, nil)
	keySpace := partitioning.NewKeySpace(2, 2) // Two key groups
	keyRange := partitioning.KeyGroupRange{Start: 0, End: 2}

	// Create a store with a tiny cache that can only hold a few items
	// Each timer key is roughly 20 bytes (2 key group + 1 schema + 8 timestamp + ~9 subject key)
	store := operator.NewTimerStore(db, keySpace, keyRange, 100)

	// Add more timers than the cache can hold
	var expectedTimestamps []time.Time
	for i := range 10 {
		ts := time.Unix(int64(i), 0)
		expectedTimestamps = append(expectedTimestamps, ts)
		store.Put(fmt.Appendf(nil, "key%d", i), ts)
	}

	timers := queues.Drain(store)
	var actualTimestamps []time.Time
	for _, timer := range timers {
		actualTimestamps = append(actualTimestamps, timer.Timestamp)
	}
	assert.Equal(t, expectedTimestamps, actualTimestamps, "should retrieve all timers in order after exceeding cache size")
}
