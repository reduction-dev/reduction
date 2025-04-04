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
	dkvstorage "reduction.dev/reduction/dkv/storage"
	"reduction.dev/reduction/proto/snapshotpb"
	"reduction.dev/reduction/storage/locations"
	"reduction.dev/reduction/storage/snapshots"
)

func TestRoundTrippingSavepoint(t *testing.T) {
	testDir := t.TempDir()
	fs := locations.NewLocal(testDir)

	checkpointEvents := make(chan snapshots.CheckpointEvent)
	store := snapshots.NewStore(&snapshots.NewStoreParams{
		CheckpointEvents: checkpointEvents,
		FileStore:        fs,
		SavepointsPath:   "savepoints",
		CheckpointsPath:  "checkpoints",
	})

	db := dkv.Open(dkv.DBOptions{
		FileSystem:   dkvstorage.NewLocalFilesystem(filepath.Join(testDir, "op1")),
		MemTableSize: 5 * 10_000,
	}, nil)
	for i := range 10_000 {
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
		FileSystem:   dkvstorage.NewLocalFilesystem(dkvstorage.Join(testDir, "op2")),
		MemTableSize: 5 * 10_000,
	}, []recovery.CheckpointHandle{cpHandle})

	// Check that all previously written entries are in the DB.
	for i := range 10_000 {
		entry, err := db.Get([]byte(strconv.Itoa(i)))
		assert.NoError(t, err)
		assert.Equal(t, entry.Value(), []byte(strconv.Itoa(i)))
	}
}

func TestObsoleteCheckpointEvents(t *testing.T) {
	fs := locations.NewLocal(t.TempDir())
	fsEvents := fs.Subscribe()
	retainedCheckpointsUpdated := make(chan []uint64)

	// Create a new store
	store := snapshots.NewStore(&snapshots.NewStoreParams{
		RetainedCheckpointsUpdated: retainedCheckpointsUpdated,
		FileStore:                  fs,
		SavepointsPath:             "savepoints",
		CheckpointsPath:            "checkpoints",
	})

	// Start the first checkpoint
	cpID1, err := store.CreateCheckpoint([]string{"op1"}, []string{"sr1"})
	require.NoError(t, err)

	// Add operator and source snapshots to complete the first checkpoint
	err = store.AddOperatorSnapshot(&snapshotpb.OperatorCheckpoint{
		CheckpointId:  cpID1,
		OperatorId:    "op1",
		DkvFileUri:    "dkv-checkpoint-file-1",
		KeyGroupRange: &snapshotpb.KeyGroupRange{Start: 0, End: 0},
	})
	require.NoError(t, err)
	err = store.AddSourceSnapshot(&snapshotpb.SourceCheckpoint{
		CheckpointId:   cpID1,
		Data:           []byte{},
		SourceRunnerId: "sr1",
	})
	require.NoError(t, err)

	// Verify that the first checkpoint file was created
	firstCkptCreated := <-fsEvents
	assert.Equal(t, locations.OpCreate, firstCkptCreated.Op)
	assert.Contains(t, firstCkptCreated.Path, ".snapshot")

	// Create and complete a second checkpoint
	cpID2, err := store.CreateCheckpoint([]string{"op1"}, []string{"sr1"})
	require.NoError(t, err)

	// Add operator and source snapshots to complete the second checkpoint
	err = store.AddOperatorSnapshot(&snapshotpb.OperatorCheckpoint{
		CheckpointId:  cpID2,
		OperatorId:    "op1",
		DkvFileUri:    "dkv-checkpoint-file-2",
		KeyGroupRange: &snapshotpb.KeyGroupRange{Start: 0, End: 0},
	})
	require.NoError(t, err)
	err = store.AddSourceSnapshot(&snapshotpb.SourceCheckpoint{
		CheckpointId:   cpID2,
		Data:           []byte{},
		SourceRunnerId: "sr1",
	})
	require.NoError(t, err)

	// Verify that the second checkpoint file was created
	secondFileEvent := <-fsEvents
	assert.Equal(t, locations.OpCreate, secondFileEvent.Op)
	assert.Contains(t, secondFileEvent.Path, ".snapshot")

	// Wait for the checkpoints retained event which should contain the second checkpoint ID
	retainedIDs := <-retainedCheckpointsUpdated
	require.Equal(t, []uint64{cpID2}, retainedIDs, "second checkpoint retained notification")

	// Verify that the first checkpoint file has been removed
	assert.Equal(t, locations.FileEvent{
		Path: firstCkptCreated.Path,
		Op:   locations.OpRemove,
	}, <-fsEvents, "first checkpoint file removed")
}
