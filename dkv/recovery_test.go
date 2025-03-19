package dkv_test

import (
	"runtime"
	"slices"
	"testing"

	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"reduction.dev/reduction/dkv"
	"reduction.dev/reduction/dkv/dkvtest"
	"reduction.dev/reduction/dkv/kv"
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
	db1Keys := dkvtest.NewHashBasedDataOwnership(2, 0)
	db1 := dkv.Open(dkv.DBOptions{
		FileSystem:    fs.WithWorkingDir("db1"),
		MemTableSize:  uint64(entryList.Size) / 3,
		DataOwnership: db1Keys,
	}, nil)
	for _, e := range entryList.Entries {
		if db1Keys.OwnsKey(e.Key()) {
			db1.Put(e.Key(), e.Value())
		}
	}

	// Checkpoint should write the checkpoint with the current WAL and SST list.
	db1CP, err := db1.Checkpoint(1)()
	require.NoError(t, err)
	require.NoError(t, db1.WaitOnTasks())

	// Start a second database
	db2Keys := dkvtest.NewHashBasedDataOwnership(2, 1)
	db2 := dkv.Open(dkv.DBOptions{
		FileSystem:    fs.WithWorkingDir("db2"),
		MemTableSize:  uint64(entryList.Size) / 3,
		DataOwnership: db2Keys,
	}, nil)
	for _, e := range entryList.Entries {
		if db2Keys.OwnsKey(e.Key()) {
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
		FileSystem:    fs.WithWorkingDir("db3"),
		MemTableSize:  uint64(entryList.Size) / 3,
		DataOwnership: dkvtest.NewHashBasedDataOwnership(1, 0), // Owns all keys
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

func TestRecoveringFromCheckpoint_Async(t *testing.T) {
	gen := dkvtest.SequenceGenerator{}
	wkDir := ksuid.New().String()
	fs := storage.NewMemoryFilesystem().WithWorkingDir(wkDir)

	dbOptions := dkv.DBOptions{FileSystem: fs, MemTableSize: 500}
	db1 := dkv.Open(dbOptions, nil)

	// Start writing
	for _, entry := range gen.All() {
		db1.Put(entry.Key, entry.Value)

		// Break when we've written flushed some data
		if len(fs.List()) > 0 {
			break
		}
	}

	// Store an exclusive end value for the checkpoint
	ckptEnd := gen.NextValue
	t.Logf("flushed; checkpointEnd=%d, files=%v", ckptEnd, fs.List())

	awaitCheckpoint := db1.Checkpoint(1)

	// Continue writing
	for _, entry := range gen.All() {
		db1.Put(entry.Key, entry.Value)

		// break once we have a checkpoint
		if slices.Contains(fs.List(), "checkpoints") {
			break
		}
	}

	// Store the last value written to the DB
	writingEnd := gen.NextValue
	t.Logf("checkpoint written; writingEnd=%d, files=%v", writingEnd, fs.List())
	ckpt, err := awaitCheckpoint()
	require.NoError(t, err)
	require.NoError(t, db1.Close())

	// Log the contents of the checkpoint
	data, _ := storage.ReadAll(fs.Open(ckpt.URI))
	t.Logf("checkpoint json: %s", string(data))

	// Start a new database from the checkpoint
	dbOptions.FileSystem = fs.WithWorkingDir("instance-2")
	db2 := dkv.Open(dbOptions, []recovery.CheckpointHandle{ckpt})
	defer db2.Close()

	gen = dkvtest.SequenceGenerator{}
	// Make sure we can read all the previously written entries
	for i, entry := range gen.All() {
		// We should have all the entries up to the checkpoint
		if i < ckptEnd {
			got, err := db2.Get(entry.Key)
			assert.NoError(t, err, "failed finding key: %d", i)
			if err == nil {
				assert.Equal(t, got.Value(), entry.Value, "incorrect key value: %d", i)
			}
		} else if i < writingEnd {
			// We shouldn't have any errors after the checkpoint
			_, err := db2.Get(entry.Key)
			assert.ErrorIs(t, err, kv.ErrNotFound, "found non-checkpointed key: %d ", i)
		} else {
			// Stop the test after we're past the previously written value
			break
		}
	}

	// Prevent garbage collection of the db
	runtime.KeepAlive(db1)
}
