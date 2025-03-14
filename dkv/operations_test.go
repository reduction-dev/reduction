package dkv_test

import (
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"reduction.dev/reduction/dkv"
	"reduction.dev/reduction/dkv/dkvtest"
	"reduction.dev/reduction/dkv/kv"
	"reduction.dev/reduction/dkv/recovery"
	"reduction.dev/reduction/dkv/storage"
)

type entry struct {
	key    string
	value  string
	delete bool
}

func TestPutAndGet(t *testing.T) {
	fs := storage.NewLocalFilesystem(t.TempDir())
	db := dkv.Open(dkv.DBOptions{FileSystem: fs}, nil)

	// Putting a key and successfully getting it.
	db.Put([]byte("k1"), []byte("v1"))
	v, err := db.Get([]byte("k1"))
	require.NoError(t, err)
	dkvtest.EntryEqual(t, dkvtest.NewKVEntry("k1", "v1"), v)

	// Trying to get a key that isn't there.
	_, err = db.Get([]byte("k2"))
	assert.ErrorIs(t, err, kv.ErrNotFound)
}

func TestScanPrefixWithNamespacedKeys(t *testing.T) {
	fs := storage.NewLocalFilesystem(t.TempDir())
	db := dkv.Open(dkv.DBOptions{FileSystem: fs}, nil)
	for _, e := range []entry{
		{key: "omit:k1", value: "a"},
		{key: "target:k2", value: "b"},
		{key: "target:k3", value: "c"},
		{key: "target:k4", value: "d"},
		{key: "omit:k2", value: "e"},
	} {
		db.Put([]byte(e.key), []byte(e.value))
	}

	var itErr error
	var values []kv.Entry
	for entry := range db.ScanPrefix([]byte("target:"), &itErr) {
		values = append(values, entry)
	}
	require.NoError(t, itErr)
	dkvtest.EntriesEqual(t, []kv.Entry{
		dkvtest.NewKVEntry("target:k2", "b"),
		dkvtest.NewKVEntry("target:k3", "c"),
		dkvtest.NewKVEntry("target:k4", "d"),
	}, values)
}

func TestScanPrefixWithExactKey(t *testing.T) {
	fs := storage.NewLocalFilesystem(t.TempDir())
	db := dkv.Open(dkv.DBOptions{FileSystem: fs}, nil)
	for _, e := range []entry{
		{key: "k1", value: "a"},
		{key: "k2", value: "b"},
		{key: "k3", value: "c"},
	} {
		db.Put([]byte(e.key), []byte(e.value))
	}

	var itErr error
	var values []kv.Entry
	for entry := range db.ScanPrefix([]byte("k2"), &itErr) {
		values = append(values, entry)
	}
	require.NoError(t, itErr)

	dkvtest.EntriesEqual(t, []kv.Entry{
		dkvtest.NewKVEntry("k2", "b"),
	}, values)
}

func TestDeleteAndGet(t *testing.T) {
	fs := storage.NewLocalFilesystem(t.TempDir())
	db := dkv.Open(dkv.DBOptions{FileSystem: fs}, nil)
	db.Put([]byte("k1"), []byte("v1"))
	db.Put([]byte("k2"), []byte("v2"))
	db.Delete([]byte("k1"))

	v, err := db.Get([]byte("k1"))
	require.NoError(t, err)
	dkvtest.EntryEqual(t, dkvtest.NewDeleteEntry("k1"), v)

	v, err = db.Get([]byte("k2"))
	require.NoError(t, err)
	dkvtest.EntryEqual(t, dkvtest.NewKVEntry("k2", "v2"), v)
}

func TestDeleteAndScanPrefix(t *testing.T) {
	fs := storage.NewLocalFilesystem(t.TempDir())
	db := dkv.Open(dkv.DBOptions{FileSystem: fs}, nil)
	for _, e := range []entry{
		{key: "pre1:k1", value: "a"},
		{key: "pre2:k2", value: "b"},
		{key: "pre2:k3", value: "c"},
		{key: "pre1:k2", value: "d"},
	} {
		db.Put([]byte(e.key), []byte(e.value))
	}

	db.Delete([]byte("pre2:k2"))

	var itErr error
	var values []kv.Entry
	for entry := range db.ScanPrefix([]byte("pre2:"), &itErr) {
		values = append(values, entry)
	}
	require.NoError(t, itErr)

	dkvtest.EntriesEqual(t, []kv.Entry{
		dkvtest.NewKVEntry("pre2:k3", "c"),
	}, values)
}

// Committing before the memtable is full, loses the data in the memtable but
// preserves the data in the log. Restarting should rebuild the memtable and
// populate the lost memtable data.
func TestCommitBeforeMemTableIsFull(t *testing.T) {
	fs := storage.NewLocalFilesystem(t.TempDir())
	db := dkv.Open(dkv.DBOptions{FileSystem: fs}, nil)
	for _, e := range []entry{
		{key: "k1", value: "a"},
		{key: "k2", value: "b"},
		{key: "k3", value: "c"},
		{key: "k4", value: "d"},
		{key: "k2", delete: true}, // Delete k2 but add it back later
		{key: "k4", delete: true}, // Only k4 should remain deleted
		{key: "k5", value: "e"},
		{key: "k2", value: "b"}, // Adding back k2
	} {
		if e.delete {
			db.Delete([]byte(e.key))
		} else {
			db.Put([]byte(e.key), []byte(e.value))
		}
	}

	ckpt, err := db.Checkpoint(1)()
	require.NoError(t, err)
	require.NoError(t, db.WaitOnTasks())

	// Initialize the database again with the checkpoint
	db = dkv.Open(dkv.DBOptions{FileSystem: fs}, []recovery.CheckpointHandle{ckpt})

	var itErr error
	var entries []kv.Entry
	for entry := range db.ScanPrefix(nil, &itErr) {
		entries = append(entries, entry)
	}
	require.NoError(t, itErr)

	dkvtest.EntriesEqual(t, []kv.Entry{
		dkvtest.NewKVEntry("k1", "a"),
		dkvtest.NewKVEntry("k2", "b"),
		dkvtest.NewKVEntry("k3", "c"),
		dkvtest.NewKVEntry("k5", "e"),
	}, entries)
}

// When the memtable gets full, dkv seals it and creates
// a new memtable.
func TestFillingMemTableStillAllowsNewWrites(t *testing.T) {
	fs := storage.NewLocalFilesystem(t.TempDir())
	db := dkv.Open(dkv.DBOptions{FileSystem: fs, MemTableSize: 1_000}, nil)
	for i := 0; i < 100; i++ {
		strBytes := []byte(fmt.Sprintf("%03d", i))
		db.Put(strBytes, strBytes)
	}

	var itErr error
	i := 0
	for entry := range db.ScanPrefix(nil, &itErr) {
		val := fmt.Sprintf("%03d", i)
		dkvtest.EntryEqual(t, dkvtest.NewKVEntry(val, val), entry)
		i++
	}
	require.NoError(t, itErr)
	assert.Equal(t, 100, i, "wrote and can read all entries")
	require.NoError(t, db.WaitOnTasks())
}

func TestFlushingMemTablesToL0AndQuerying(t *testing.T) {
	entryList := dkvtest.RandomEntriesList(10)

	// Build a memtable that exactly fits the list of entries and fill it.
	tDir := t.TempDir()
	fs := storage.NewLocalFilesystem(tDir)
	db := dkv.Open(dkv.DBOptions{FileSystem: fs, MemTableSize: uint64(entryList.Size)}, nil)
	for _, e := range entryList.Entries {
		db.Put(e.Key(), e.Value())
	}

	// The next put should produce a sealed memtable and schedule a flush.
	db.Put([]byte("key"), []byte("value"))
	require.NoError(t, db.WaitOnTasks())

	// There should be one sst file after the flush.
	assert.Equal(t, 1, dkvtest.SSTFileCount(t, tDir))

	// Should be able to query keys in L0 SST table.
	entry, err := db.Get(entryList.At(0).Key())
	require.NoError(t, err)
	dkvtest.EntryEqual(t, entryList.At(0), entry)

	// Should be able to scan for keys in memTables and L0 table.
	memEntryWithPrefix := dkvtest.NewKVEntry(string(append(entryList.At(0).Key(), []byte(":1")...)), "mem-value")
	db.Put(memEntryWithPrefix.Key(), memEntryWithPrefix.Value())

	var itErr error
	values := slices.Collect(db.ScanPrefix(entryList.At(0).Key(), &itErr))
	require.NoError(t, itErr)

	// Scan entries should contain the item flushed to SST and item in memtable
	// with matching prefix demonstrating that we queried both the SST and the
	// memTable.
	dkvtest.EntriesEqual(t, []kv.Entry{entryList.At(0), memEntryWithPrefix}, values)

	// Newer writes to memTable are returned from Get.
	memEntryOverwrite := dkvtest.NewKVEntry(string(entryList.At(0).Key()), "new-value")
	db.Put(memEntryOverwrite.Key(), memEntryOverwrite.Value())
	entry, err = db.Get([]byte(entryList.At(0).Key()))
	require.NoError(t, err)
	dkvtest.EntryEqual(t, memEntryOverwrite, entry)
}

func TestRemovingCheckpoints(t *testing.T) {
	entryList := dkvtest.SequentialEntriesList(100)
	fs := storage.NewMemoryFilesystem()

	// Make the memtable half the size of the entry list so we end up with about half in
	// memory and half in a flushed L0 table.
	db := dkv.Open(dkv.DBOptions{
		FileSystem:                  fs,
		MemTableSize:                uint64(entryList.Size) / 2,
		L0TableNumCompactionTrigger: 999, // Avoid compaction for this test
	}, nil)
	for _, e := range entryList.Entries {
		db.Put(e.Key(), e.Value())
	}
	require.NoError(t, db.WaitOnTasks())

	// Checkpoint should write the checkpoint with the current WAL and SST list.
	cpLocation, err := db.Checkpoint(1)()
	assert.Equal(t, recovery.CheckpointHandle{
		CheckpointID: 1,
		URI:          "memory:///checkpoints",
	}, cpLocation)
	require.NoError(t, err)
	require.NoError(t, db.WaitOnTasks())

	assert.Equal(t, []string{
		"000000.sst",
		"000000.wal",
		"checkpoints",
	}, fs.List(), "fs has one table, the recovered wal, and the checkpoints file")

	// Write enough items to create another SST table after the checkpoint
	for _, e := range dkvtest.SequentialEntriesList(50).Entries {
		db.Put(e.Key(), e.Value())
	}
	require.NoError(t, db.WaitOnTasks())

	newCheckpoint, err := db.Checkpoint(2)()
	require.NoError(t, err)
	require.NoError(t, db.WaitOnTasks())

	assert.Equal(t, []string{
		"000000.sst",
		"000000.wal",
		"000001.sst",
		"000001.wal",
		"checkpoints",
	}, fs.List(), "fs has one table, the recovered wal, and the checkpoints file")

	db.UpdateRetainedCheckpoints([]uint64{newCheckpoint.CheckpointID})
	assert.Equal(t, []string{
		"000000.sst",
		"000001.sst",
		"000001.wal",
		"checkpoints",
	}, fs.List(), "fs has one table, the recovered wal, and the checkpoints file")
}

// This test shows that writing identical entries many times does not consume
// space within a single memtable. This is important for timers where rewriting
// the same timer is a common use case.
func TestWritingSameEntryDoesNotConsumeStorage(t *testing.T) {
	fs := storage.NewLocalFilesystem(t.TempDir())
	db := dkv.Open(dkv.DBOptions{
		FileSystem:   fs,
		MemTableSize: 1000, // Small memtable size to trigger flush if space is used
	}, nil)

	// Write the same key 1000 times, which would exceed memtable size if each write
	// consumed space
	for i := 0; i < 1000; i++ {
		db.Put([]byte("key"), []byte("value"))
	}
	require.NoError(t, db.WaitOnTasks())

	assert.Empty(t, fs.List(),
		"no SST files should be created since repeated writes don't accumulate in memtable")

	// Verify we can still read the final value
	entry, err := db.Get([]byte("key"))
	require.NoError(t, err)
	dkvtest.EntryEqual(t, dkvtest.NewKVEntry("key", "value"), entry)
}

func TestReachingMaxWALSize(t *testing.T) {
	fs := storage.NewLocalFilesystem(t.TempDir())
	db := dkv.Open(dkv.DBOptions{
		FileSystem: fs,
		MaxWALSize: 100,
	}, nil)

	// Write enough to hit the WAL size limit
	for i := range 5 {
		db.Put([]byte("key"), fmt.Appendf(nil, "%03d", i))
	}
	require.NoError(t, db.WaitOnTasks())

	assert.Equal(t, []string{
		"000000.sst",
	}, fs.List(), "flushes one SST file")
}
