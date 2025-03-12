package dkv_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"reduction.dev/reduction/dkv"
	"reduction.dev/reduction/dkv/dkvtest"
	"reduction.dev/reduction/dkv/recovery"
	"reduction.dev/reduction/dkv/storage"
	"reduction.dev/reduction/util/size"
)

func TestRecoveringFromCheckpoint(t *testing.T) {
	entryList := dkvtest.SequentialEntriesList(100)
	fs := storage.NewMemoryFilesystem()

	// Make the memtable half the size of the entry list so we end up with about half in
	// memory and half in a flushed L0 table.
	dbOptions := dkv.DBOptions{FileSystem: fs, MemTableSize: uint64(entryList.Size) / 2}
	db := dkv.Open(dbOptions, nil)
	for _, e := range entryList.Entries {
		db.Put(e.Key(), e.Value())
	}
	require.NoError(t, db.WaitOnTasks())

	// Checkpoint should write the checkpoint with the current WAL and SST list.
	ckpt, err := db.Checkpoint(1)()
	require.NoError(t, err)
	require.NoError(t, db.WaitOnTasks())

	assert.Equal(t, []string{
		"000000.sst",
		"000000.wal",
		"checkpoints",
	}, fs.List(), "fs has one table, the recovered wal, and the checkpoints file")

	// Start a new database which should recover from the checkpoint
	db = dkv.Open(dbOptions, []recovery.CheckpointHandle{ckpt})

	// Make sure we can read all the previously written entries
	for _, entry := range entryList.Entries {
		got, err := db.Get(entry.Key())
		require.NoError(t, err)
		assert.Equal(t, got.Value(), entry.Value())
	}

	_, err = db.Checkpoint(1)()
	require.NoError(t, err)
	require.NoError(t, db.WaitOnTasks())

	assert.Equal(t, []string{
		"000000.sst",
		"000000.wal",
		"000001.wal",
		"checkpoints",
	}, fs.List(), "fs has one table, the recovered wal, and the checkpoints file")
}

func TestRecoveringFromCheckpointBeforeAnyMemtableFlush(t *testing.T) {
	entryList := dkvtest.SequentialEntriesList(10)
	fs := storage.NewMemoryFilesystem()

	dbOptions := dkv.DBOptions{FileSystem: fs, MemTableSize: 1 * size.MB}
	db := dkv.Open(dbOptions, nil)
	for _, e := range entryList.Entries {
		db.Put(e.Key(), e.Value())
	}
	require.NoError(t, db.WaitOnTasks())

	// Checkpoint should write the checkpoint with the current WAL and SST list.
	ckpt, err := db.Checkpoint(1)()
	require.NoError(t, err)

	// Saves a checkpoint in the provided path
	assert.Equal(t, []string{
		"000000.wal",
		"checkpoints",
	}, fs.List(), "fs has the wal, and the checkpoints file")

	// Start a new database which should recover from the checkpoint
	db = dkv.Open(dbOptions, []recovery.CheckpointHandle{ckpt})

	// Make sure we can read all the previously written entries
	for _, entry := range entryList.Entries {
		got, err := db.Get(entry.Key())
		require.NoError(t, err)
		assert.Equal(t, got.Value(), entry.Value())
	}
}

func TestRecoveringFromTwoCheckpoints(t *testing.T) {
	entryList := dkvtest.SequentialEntriesList(100)
	fs := storage.NewMemoryFilesystem()

	// Process entries with first DB
	db1Partition := dkvtest.NewPartition(2, 0)
	db1 := dkv.Open(dkv.DBOptions{
		FileSystem:   fs.WithWorkingDir("db1"),
		MemTableSize: uint64(entryList.Size) / 3,
		Partition:    db1Partition,
	}, nil)
	for _, e := range entryList.Entries {
		if db1Partition.OwnsKey(e.Key()) {
			db1.Put(e.Key(), e.Value())
		}
	}

	// Checkpoint should write the checkpoint with the current WAL and SST list.
	db1CP, err := db1.Checkpoint(1)()
	require.NoError(t, err)
	require.NoError(t, db1.WaitOnTasks())

	// Start a second database
	db2Partition := dkvtest.NewPartition(2, 1)
	db2 := dkv.Open(dkv.DBOptions{
		FileSystem:   fs.WithWorkingDir("db2"),
		MemTableSize: uint64(entryList.Size) / 3,
		Partition:    db2Partition,
	}, nil)
	for _, e := range entryList.Entries {
		if db2Partition.OwnsKey(e.Key()) {
			db2.Put(e.Key(), e.Value())
		}
	}

	// Add to distributed cp1 checkpoint
	db2CP, err := db2.Checkpoint(1)()
	require.NoError(t, err)
	require.NoError(t, db2.WaitOnTasks())

	// Produced 2 sets of checkpoint files
	assert.Equal(t, []string{
		"db1/000000.sst",
		"db1/000000.wal",
		"db1/checkpoints",
		"db2/000000.sst",
		"db2/000000.wal",
		"db2/checkpoints",
	}, fs.List(), "fs has one table, the recovered wal, and the checkpoints file")

	// Start a new DB that reads from the previous 2 checkpoints
	db3 := dkv.Open(dkv.DBOptions{
		FileSystem:   fs.WithWorkingDir("db3"),
		MemTableSize: uint64(entryList.Size) / 3,
		Partition:    dkvtest.NewPartition(1, 0), // Owns all keys
	}, []recovery.CheckpointHandle{db1CP, db2CP})

	// Make sure we can read all the previously written entries
	for _, entry := range entryList.Entries {
		got, err := db3.Get(entry.Key())
		require.NoError(t, err, "key: %s", entry.Key())
		assert.Equal(t, got.Value(), entry.Value())
	}
}

func TestRecoveringFromSubsequentCheckpointWithNoSSTs(t *testing.T) {
	entryList := dkvtest.SequentialEntriesList(200)
	firstEntries, secondEntries := entryList.Entries[:100], entryList.Entries[100:]
	fs := storage.NewMemoryFilesystem()

	// Set high enough memory that we don't trigger any memtable flushed in this test
	dbOptions := dkv.DBOptions{FileSystem: fs, MemTableSize: 1 * size.MB}
	db := dkv.Open(dbOptions, nil)

	// Write first list of entries
	for _, e := range firstEntries {
		db.Put(e.Key(), e.Value())
	}
	require.NoError(t, db.WaitOnTasks())

	// 1st checkpoint
	_, err := db.Checkpoint(1)()
	require.NoError(t, err)
	require.NoError(t, db.WaitOnTasks())

	assert.Equal(t, []string{
		"000000.wal",
		"checkpoints",
	}, fs.List(), "only the WAL is written")

	// Write second list of entries
	for _, e := range secondEntries {
		db.Put(e.Key(), e.Value())
	}
	require.NoError(t, db.WaitOnTasks())

	// 2nd checkpoint
	ckpt2, err := db.Checkpoint(2)()
	require.NoError(t, err)
	require.NoError(t, db.WaitOnTasks())

	assert.Equal(t, []string{
		"000000.wal",
		"000001.wal",
		"checkpoints",
	}, fs.List(), "new wal written")

	// Start a new database which should recover from the 2nd checkpoint
	db = dkv.Open(dbOptions, []recovery.CheckpointHandle{ckpt2})

	// Make sure we can read all the previously written entries
	for _, entry := range entryList.Entries {
		got, err := db.Get(entry.Key())
		require.NoError(t, err)
		assert.Equal(t, got.Value(), entry.Value())
	}
}

func TestRecoveringFromTwoSubsequentCheckpointsWithNoSSTs(t *testing.T) {
	entryList := dkvtest.SequentialEntriesList(200)
	firstEntries, secondEntries := entryList.Entries[:100], entryList.Entries[100:]
	fs := storage.NewMemoryFilesystem()

	// Set high enough memory that we don't trigger any memtable flushed in this test
	dbOptions := dkv.DBOptions{FileSystem: fs, MemTableSize: 1 * size.MB}
	db := dkv.Open(dbOptions, nil)

	// Write first list of entries
	for _, e := range firstEntries {
		db.Put(e.Key(), e.Value())
	}
	require.NoError(t, db.WaitOnTasks())

	// 1st checkpoint
	ckpt1, err := db.Checkpoint(1)()
	require.NoError(t, err)
	require.NoError(t, db.WaitOnTasks())

	assert.Equal(t, []string{
		"000000.wal",
		"checkpoints",
	}, fs.List(), "only the WAL is written")

	// Start a new database from 1st checkpoint
	db = dkv.Open(dbOptions, []recovery.CheckpointHandle{ckpt1})

	// Write second list of entries
	for _, e := range secondEntries {
		db.Put(e.Key(), e.Value())
	}
	require.NoError(t, db.WaitOnTasks())

	// 2nd checkpoint
	ckpt2, err := db.Checkpoint(2)()
	require.NoError(t, err)
	require.NoError(t, db.WaitOnTasks())

	assert.Equal(t, []string{
		"000000.wal",
		"000001.wal",
		"checkpoints",
	}, fs.List(), "new wal written")

	// Start a new database which should recover from the 2nd checkpoint
	db = dkv.Open(dbOptions, []recovery.CheckpointHandle{ckpt2})

	// Make sure we can read all the previously written entries
	for _, entry := range entryList.Entries {
		got, err := db.Get(entry.Key())
		require.NoError(t, err)
		assert.Equal(t, got.Value(), entry.Value())
	}
}
