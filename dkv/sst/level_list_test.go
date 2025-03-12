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

func TestHoldingAndDroppingLevelListRef(t *testing.T) {
	fs := storage.NewMemoryFilesystem()
	f := fs.New("file-1")

	ll := sst.NewEmptyLevelList(1)
	table := sst.NewTable(f)
	ll.AddTables(0, table)
	require.NoError(t, ll.DropRef())

	assert.False(t, fs.Exists(f.Name()))
}

func TestHoldingAndDroppingMultipleLevelListRefs(t *testing.T) {
	fs := storage.NewMemoryFilesystem()
	f1 := fs.New("file-1")
	f2 := fs.New("file-2")

	// Build first LevelList with a table1
	table1 := sst.NewTable(f1)
	cs := &sst.ChangeSet{}
	cs.AddTables(0, table1)
	ll1 := sst.NewEmptyLevelList(1).NewWithChangeSet(cs)

	// Build second LevelList with table1 & table2
	table2 := sst.NewTable(f2)
	cs = &sst.ChangeSet{}
	cs.AddTables(0, table2)
	ll2 := ll1.NewWithChangeSet(cs)

	require.NoError(t, ll1.DropRef())
	require.NoError(t, ll2.DropRef())

	assert.False(t, fs.Exists(f1.Name()))
	assert.False(t, fs.Exists(f2.Name()))
}

func TestPickingL0TablesForKey(t *testing.T) {
	tw := sst.NewTableWriter(storage.NewMemoryFilesystem(), 0)
	tables := make([]*sst.Table, 4)
	for i, seq := range [][]kv.Entry{
		{dkvtest.NewKVEntry("1", ""), dkvtest.NewKVEntry("2", "")},
		{dkvtest.NewKVEntry("2", ""), dkvtest.NewKVEntry("3", "")},
		{dkvtest.NewKVEntry("4", ""), dkvtest.NewKVEntry("5", "")},
		{dkvtest.NewKVEntry("3", ""), dkvtest.NewKVEntry("4", "")},
	} {
		table, err := tw.Write(slices.Values(seq))
		require.NoError(t, err)
		tables[i] = table
	}

	ll := sst.NewLevelListOfTables([][]*sst.Table{tables})
	keyTables := slices.Collect(ll.AllTablesForKey([]byte("3")))

	assert.Len(t, keyTables, 2)
	assert.Same(t, keyTables[0], tables[1])
	assert.Same(t, keyTables[1], tables[3])
}

func TestPickingL1ToLMaxTablesForKey(t *testing.T) {
	tw := sst.NewTableWriter(storage.NewMemoryFilesystem(), 0)
	tables := make([]*sst.Table, 3)
	for i, seq := range [][]kv.Entry{
		{dkvtest.NewKVEntry("1", ""), dkvtest.NewKVEntry("2", "")},
		{dkvtest.NewKVEntry("3", ""), dkvtest.NewKVEntry("4", "")},
		{dkvtest.NewKVEntry("5", ""), dkvtest.NewKVEntry("6", "")},
	} {
		table, err := tw.Write(slices.Values(seq))
		require.NoError(t, err)
		tables[i] = table
	}

	ll := sst.NewLevelListOfTables([][]*sst.Table{nil, tables})
	keyTables := slices.Collect(ll.AllTablesForKey([]byte("3")))

	assert.Len(t, keyTables, 1)
	assert.Same(t, keyTables[0], tables[1])
}

func TestPickingL0TablesForPrefix(t *testing.T) {
	tw := sst.NewTableWriter(storage.NewMemoryFilesystem(), 0)
	entrySeqs := [][]kv.Entry{
		{dkvtest.NewKVEntry("b", ""), dkvtest.NewKVEntry("bc", "")},  // All b prefix, earliest start GOT
		{dkvtest.NewKVEntry("bc", ""), dkvtest.NewKVEntry("bd", "")}, // All b prefix, middle start
		{dkvtest.NewKVEntry("a", ""), dkvtest.NewKVEntry("ab", "")},  // No b contained, earlier
		{dkvtest.NewKVEntry("a", ""), dkvtest.NewKVEntry("b", "")},   // Ends with b prefix GOT
		{dkvtest.NewKVEntry("c", ""), dkvtest.NewKVEntry("d", "")},   // No b container, later
		{dkvtest.NewKVEntry("a", ""), dkvtest.NewKVEntry("bz", "")},  // No b container but ends with prefix
	}
	tables := make([]*sst.Table, len(entrySeqs))
	for i, seq := range entrySeqs {
		table, err := tw.Write(slices.Values(seq))
		require.NoError(t, err)
		tables[i] = table
	}

	ll := sst.NewLevelListOfTables([][]*sst.Table{tables})
	prefixTables := slices.Collect(ll.AllTablesForPrefix([]byte("b")))

	assert.Len(t, prefixTables, 4)
	assert.Same(t, prefixTables[0], tables[0])
	assert.Same(t, prefixTables[1], tables[1])
	assert.Same(t, prefixTables[2], tables[3])
	assert.Same(t, prefixTables[3], tables[5])
}

func TestPickingL1ToLMaxTablesForPrefix(t *testing.T) {
	tw := sst.NewTableWriter(storage.NewMemoryFilesystem(), 0)
	leveledSeqs := [][][]kv.Entry{{}, {
		{dkvtest.NewKVEntry("a", ""), dkvtest.NewKVEntry("aa", "")}, // Too early
		{dkvtest.NewKVEntry("b", ""), dkvtest.NewKVEntry("bb", "")}, // Has Prefix
		{dkvtest.NewKVEntry("c", ""), dkvtest.NewKVEntry("cc", "")}, // Too late
	}, {
		{dkvtest.NewKVEntry("a", ""), dkvtest.NewKVEntry("c", "")}, // Contains prefix
		{dkvtest.NewKVEntry("d", ""), dkvtest.NewKVEntry("e", "")}, // Too Late
	}, {
		{dkvtest.NewKVEntry("a", ""), dkvtest.NewKVEntry("bb", "")}, // Has Prefix
		{dkvtest.NewKVEntry("bc", ""), dkvtest.NewKVEntry("c", "")}, // Prefix continues into next level
	}}
	levels := make([][]*sst.Table, len(leveledSeqs))
	for i, level := range leveledSeqs {
		levels[i] = make([]*sst.Table, len(level))
		for j, seq := range level {
			table, err := tw.Write(slices.Values(seq))
			require.NoError(t, err)
			levels[i][j] = table
		}
	}

	ll := sst.NewLevelListOfTables(levels)
	prefixTables := slices.Collect(ll.AllTablesForPrefix([]byte("b")))

	assert.Len(t, prefixTables, 4)
	assert.Same(t, prefixTables[0], levels[1][1]) // Reminder: L0 is empty
	assert.Same(t, prefixTables[1], levels[2][0])
	assert.Same(t, prefixTables[2], levels[3][0])
	assert.Same(t, prefixTables[3], levels[3][1])
}

func TestScanPrefixFiltersDeletedEntries(t *testing.T) {
	tw := sst.NewTableWriter(storage.NewMemoryFilesystem(), 0)
	entries := []kv.Entry{
		dkvtest.NewKVEntry("b1", "value1"),
		dkvtest.NewDeleteEntry("b2"),
		dkvtest.NewKVEntry("b3", "value3"),
		dkvtest.NewDeleteEntry("b4"),
		dkvtest.NewKVEntry("c1", "value-outside"),
	}
	table, err := tw.Write(slices.Values(entries))
	require.NoError(t, err)
	ll := sst.NewLevelListOfTables([][]*sst.Table{{table}})

	var errOut error
	scannedEntries := slices.Collect(ll.ScanPrefix([]byte("b"), &errOut))
	require.NoError(t, errOut)

	// Should only return non-deleted entries with prefix "b"
	assert.Len(t, scannedEntries, 2)
	assert.Equal(t, []byte("b1"), scannedEntries[0].Key())
	assert.Equal(t, []byte("b3"), scannedEntries[1].Key())
}
