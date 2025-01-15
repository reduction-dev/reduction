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
	"reduction.dev/reduction/util/size"
)

func TestCompaction_NoTablesWritten(t *testing.T) {
	c := sst.Compactor{
		L0RunNumCompactionTrigger: 1,
	}

	// Compacting empty set of tables results in no compaction
	ll := sst.NewEmptyLevelList(2)
	cs, err := c.Compact(ll)
	require.NoError(t, err)
	assert.Nil(t, cs)
}

func TestCompaction_Major(t *testing.T) {
	tw := sst.NewTableWriter(storage.NewMemoryFilesystem(), 0)
	ll := sst.NewEmptyLevelList(2)
	c := sst.Compactor{
		TableWriter:                 tw,
		L0RunNumCompactionTrigger:   2,
		MaxSizeAmplificationPercent: 50,
		TargetTableSize:             5 * size.MB,
	}

	table1Entries := slices.SortedFunc(dkvtest.RandomEntriesSeq(100), kv.AscendingEntries)
	table, err := tw.Write(slices.Values(table1Entries))
	require.NoError(t, err)

	// With one L0 table, haven't reached threshold for compaction yet
	ll.AddTables(0, table)
	cs, err := c.Compact(ll)
	require.NoError(t, err)
	assert.Nil(t, cs)
	sar := ll.SizeAmplificationRatio()
	assert.Equal(t, sst.SizeAmplificationMax, sar.Percentage())

	// At two tables with the same size have reached threshold for compaction
	table2Entries := slices.SortedFunc(dkvtest.RandomEntriesSeq(100), kv.AscendingEntries)
	table, err = tw.Write(slices.Values(table2Entries))
	require.NoError(t, err)
	ll.AddTables(0, table)
	cs, err = c.Compact(ll)
	require.NoError(t, err)
	assert.NotNil(t, cs, "returns a changeset")

	// Size amplification ration adjusted after applying changeset.
	ll = ll.NewWithChangeSet(cs)
	sar = ll.SizeAmplificationRatio()
	assert.Equal(t, 0, sar.Percentage())

	// Query keys in the newly created table
	t.Log(ll.Diagnostics())
	baseTable := slices.Collect(ll.At(1).AllTables())[0]
	t.Log(baseTable.Diagnostics())

	for _, e := range table1Entries {
		gotEntry, err := baseTable.Get(e.Key())
		require.NoErrorf(t, err, "tried key: %s", e.Key())
		require.Equal(t, e.Value(), gotEntry.Value())
	}

	for _, e := range table2Entries {
		gotEntry, err := baseTable.Get(e.Key())
		require.NoError(t, err)
		require.Equal(t, e.Value(), gotEntry.Value())
	}
}

func TestCompaction_Minor(t *testing.T) {
	ll := sst.NewEmptyLevelList(3)
	tw := sst.NewTableWriter(storage.NewMemoryFilesystem(), 0)

	// Add to L0 tables, which will trigger minor compaction
	table1, err := tw.Write(dkvtest.RandomEntriesSeq(100))
	require.NoError(t, err)
	table2, err := tw.Write(dkvtest.RandomEntriesSeq(100))
	require.NoError(t, err)
	ll.AddTables(0, table1, table2)

	// Preconditions
	assert.Equal(t, []int{2, 0, 0}, ll.TableCounts())

	c := sst.Compactor{
		TableWriter:                 tw,
		L0RunNumCompactionTrigger:   2,
		MaxSizeAmplificationPercent: sst.SizeAmplificationMax,
		SmallestLevelSize:           table1.Size() * 2, // Allow size of two tables to fit in L1
		LevelSizeMultiplier:         2,
		TargetTableSize:             5 * size.MB,
	}

	// Run minor compaction
	for {
		cs, err := c.Compact(ll)
		require.NoError(t, err)
		if cs == nil {
			break
		}
		ll = ll.NewWithChangeSet(cs)
	}
	assert.Equal(t, []int{0, 1, 0}, ll.TableCounts())

	// Add to three more L0 tables, which will trigger minor compaction again.
	table1, err = tw.Write(dkvtest.RandomEntriesSeq(100))
	require.NoError(t, err)
	table2, err = tw.Write(dkvtest.RandomEntriesSeq(100))
	require.NoError(t, err)
	table3, err := tw.Write(dkvtest.RandomEntriesSeq(100))
	require.NoError(t, err)
	ll.AddTables(0, table1, table2, table3)

	// Preconditions
	assert.Equal(t, []int{3, 1, 0}, ll.TableCounts())

	// Trigger minor compaction
	for {
		cs, err := c.Compact(ll)
		require.NoError(t, err)
		if cs == nil {
			break
		}
		t.Log("applying changeset: ", cs)
		ll = ll.NewWithChangeSet(cs)
	}
	assert.Equal(t, []int{0, 0, 1}, ll.TableCounts())
}
