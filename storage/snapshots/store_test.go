package snapshots_test

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"reduction.dev/reduction/dkv"
	"reduction.dev/reduction/dkv/recovery"
	"reduction.dev/reduction/dkv/storage"
	"reduction.dev/reduction/proto/snapshotpb"
	"reduction.dev/reduction/storage/localfs"
	"reduction.dev/reduction/storage/snapshots"
)

func TestRoundTrippingSavepoint(t *testing.T) {
	testDir := t.TempDir()
	fs := localfs.NewDirectory(testDir)

	checkpointEvents := make(chan snapshots.CheckpointEvent)
	store := snapshots.NewStore(&snapshots.NewStoreParams{
		CheckpointEvents: checkpointEvents,
		FileStore:        fs,
		SavepointsPath:   "savepoints",
		CheckpointsPath:  "checkpoints",
	})

	db := dkv.Open(dkv.DBOptions{
		FileSystem:   storage.NewLocalFilesystem(filepath.Join(testDir, "op1")),
		MemTableSize: 5 * 10_000,
	}, nil)
	for i := 0; i < 10_000; i++ {
		db.Put([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i)))
	}
	cpHandle, err := db.Checkpoint(1)()
	require.NoError(t, err)

	cpID, created, err := store.CreateSavepoint([]string{"op1"}, []string{"sr1"})
	require.NoError(t, err)
	assert.True(t, created)

	err = store.AddOperatorSnapshot(&snapshotpb.OperatorCheckpoint{
		CheckpointId:  cpID,
		OperatorId:    "op1",
		DkvFileUri:    cpHandle.URI,
		KeyGroupRange: &snapshotpb.KeyGroupRange{Start: 0, End: 0},
	})
	require.NoError(t, err)

	err = store.AddSourceSnapshot(&snapshotpb.SourceCheckpoint{
		CheckpointId:   cpID,
		Data:           []byte{},
		SourceRunnerId: "sr1",
	})
	require.NoError(t, err)

	// Wait for the savepoint to be created
	result := <-checkpointEvents
	require.NoError(t, result.Err)

	spURI, err := store.SavepointURIForID(cpID)
	require.NoError(t, err)

	// Create a new store like we're booting from scratch
	store = snapshots.NewStore(&snapshots.NewStoreParams{
		FileStore:       fs,
		SavepointsPath:  "savepoints",
		CheckpointsPath: "checkpoints",
		SavepointURI:    spURI,
	})

	// Remove the previous checkpoint files. These will be replaced by LoadLatestCheckpoint.
	os.RemoveAll(filepath.Join(testDir, "op1"))

	_, err = store.LoadLatestCheckpoint()
	require.NoError(t, err)

	db = dkv.Open(dkv.DBOptions{
		FileSystem:   storage.NewLocalFilesystem(filepath.Join(testDir, "op2")),
		MemTableSize: 5 * 10_000,
	}, []recovery.CheckpointHandle{cpHandle})

	// Check that all previously written entries are in the DB.
	for i := 0; i < 10_000; i++ {
		entry, err := db.Get([]byte(strconv.Itoa(i)))
		assert.NoError(t, err)
		assert.Equal(t, entry.Value(), []byte(strconv.Itoa(i)))
	}
}
