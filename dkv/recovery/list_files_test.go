package recovery_test

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"reduction.dev/reduction/dkv/kv"
	"reduction.dev/reduction/dkv/recovery"
	"reduction.dev/reduction/dkv/sst"
	"reduction.dev/reduction/dkv/storage"
	"reduction.dev/reduction/dkv/wal"
)

func TestListFiles(t *testing.T) {
	fs := storage.NewMemoryFilesystem()
	cl := recovery.NewCheckpointList()
	tw := sst.NewTableWriter(fs, 0)
	ll := sst.NewEmptyLevelList(1)

	// Add a table to the level list
	table, err := tw.Write(slices.Values[[]kv.Entry](nil))
	require.NoError(t, err)
	ll.AddTables(0, table)

	// Create a checkpoint
	cl.Add(1, ll, wal.NewWriter(fs, 0))
	uri, err := cl.Save(fs)
	require.NoError(t, err)
	assert.Equal(t, "memory:///checkpoints", uri)

	// List the checkpoints files
	reader := &storage.Cursor{File: fs.Open(uri)}
	files, err := recovery.ListFiles(reader)
	require.NoError(t, err)
	assert.Equal(t, []string{
		"memory:///000000.wal",
		"000000.sst",
	}, files)
}
