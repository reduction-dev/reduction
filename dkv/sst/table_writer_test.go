package sst_test

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"reduction.dev/reduction/dkv/dkvtest"
	"reduction.dev/reduction/dkv/kv"
	"reduction.dev/reduction/dkv/sst"
	"reduction.dev/reduction/dkv/storage"
)

func TestGet(t *testing.T) {
	tw := sst.NewTableWriter(storage.NewMemoryFilesystem(), 0)

	writtenEntry := dkvtest.NewKVEntry("key", "value")
	entries := slices.Values([]kv.Entry{writtenEntry})
	table, err := tw.Write(entries)
	require.NoError(t, err)

	v, err := table.Get([]byte("key"))
	require.NoError(t, err)
	dkvtest.EntryEqual(t, writtenEntry, v)
}

// Given entries that can be perfectly split between tables we expect tables of
// uniform size.
func TestWriteTables_PerfectSizing(t *testing.T) {
	tw := sst.NewTableWriter(storage.NewMemoryFilesystem(), 0)

	entryList := dkvtest.RandomEntriesList(4)
	tables, err := tw.WriteRun(slices.Values(entryList.Entries), uint64(entryList.Size)/2)
	require.NoError(t, err)
	assert.Len(t, tables, 2)
	assert.Equal(t, tables[0].Size(), tables[1].Size())
}

// Given entries that end just over the target size, we expect the last table to
// be bigger than the target size.
func TestWriteTables_JustOverTargetSize(t *testing.T) {
	tw := sst.NewTableWriter(storage.NewMemoryFilesystem(), 0)

	firstChunk := dkvtest.RandomEntriesList(3)
	secondChunk := dkvtest.RandomEntriesList(1)
	allEntries := slices.Concat(firstChunk.Entries, secondChunk.Entries)
	tables, err := tw.WriteRun(slices.Values(allEntries), uint64(firstChunk.Size))
	require.NoError(t, err)
	assert.Len(t, tables, 1)
	assert.Equal(t, int64(firstChunk.Size+secondChunk.Size), tables[0].EntriesSize())
}

// Given entries that end just under the target size, we expect the last table to
// be smaller than the target size.
func TestWriteTables_JustUnderTargetSize(t *testing.T) {
	tw := sst.NewTableWriter(storage.NewMemoryFilesystem(), 0)

	chunk0, chunk1, chunk2 := dkvtest.RandomEntriesList(3), dkvtest.RandomEntriesList(3), dkvtest.RandomEntriesList(2)
	allEntries := slices.Concat(chunk0.Entries, chunk1.Entries, chunk2.Entries)
	tables, err := tw.WriteRun(slices.Values(allEntries), uint64(chunk0.Size))
	require.NoError(t, err)

	assert.Len(t, tables, 3)
	assert.Equal(t, int64(chunk0.Size), tables[0].EntriesSize())
	assert.Equal(t, int64(chunk1.Size), tables[1].EntriesSize())
	assert.Equal(t, int64(chunk2.Size), tables[2].EntriesSize())
	assert.Less(t, tables[2].Size(), tables[1].Size())
}

// This was a difficult bug where if the last entry in a table was deleted, the
// scan would error with EOF.
func TestWrite_SingleDeletedEntryDoesNotError(t *testing.T) {
	tw := sst.NewTableWriter(storage.NewMemoryFilesystem(), 0)

	// Create just a deleted entry
	entries := slices.Values([]kv.Entry{dkvtest.NewDeleteEntry("key")})

	// Write the entry to the table
	table, err := tw.Write(entries)
	require.NoError(t, err)

	// Ensure we can read to the end without EOF errors
	var scanErr error
	scanIter := table.ScanPrefix([]byte{}, &scanErr)
	_ = slices.Collect(scanIter)
	assert.NoError(t, scanErr, "Should not get EOF when scanning deleted entries")
}

// Test scanning a table with a deleted entry that doesn't match the prefix.
func TestScanPrefix_SkipDeletedNonMatchingEntry(t *testing.T) {
	tw := sst.NewTableWriter(storage.NewMemoryFilesystem(), 0)

	// Create entries where with one deleted entry that doesn't match the prefix
	entries := slices.Values([]kv.Entry{
		dkvtest.NewKVEntry("prefix1", "value1"),
		dkvtest.NewDeleteEntry("no-match"),
		dkvtest.NewKVEntry("prefix2", "value2"),
	})

	// Write the entries to the table
	table, err := tw.Write(entries)
	require.NoError(t, err)

	var scanErr error
	scanIter := table.ScanPrefix([]byte("prefix"), &scanErr)
	results := slices.Collect(scanIter)

	// Verify no error occurred during scan
	assert.NoError(t, scanErr, "Should not error when skipping deleted entry")

	// Verify we got the expected matching entries
	dkvtest.EntriesEqual(t, []kv.Entry{
		dkvtest.NewKVEntry("prefix1", "value1"),
		dkvtest.NewKVEntry("prefix2", "value2"),
	}, results)
}
