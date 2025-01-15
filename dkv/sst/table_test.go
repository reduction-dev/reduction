package sst_test

import (
	"slices"
	"testing"

	"github.com/VictoriaMetrics/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"reduction.dev/reduction/dkv/dkvtest"
	"reduction.dev/reduction/dkv/kv"
	"reduction.dev/reduction/dkv/sst"
	"reduction.dev/reduction/dkv/storage"
)

func TestTable_Get(t *testing.T) {
	tw := sst.NewTableWriter(storage.NewMemoryFilesystem(), 0)

	entries := dkvtest.SequentialEntriesSeq(16_000, 0)
	table, err := tw.Write(entries)
	require.NoError(t, err)

	entriesList := slices.Collect(entries)
	for i := 0; i < len(entriesList); i += 7 {
		expectedEntry := entriesList[i]
		entry, err := table.Get(expectedEntry.Key())
		require.NoError(t, err)
		dkvtest.EntryEqual(t, expectedEntry, entry)
	}
}

func TestTable_Load(t *testing.T) {
	fs := storage.NewMemoryFilesystem()
	tw := sst.NewTableWriter(fs, 0)
	entries := dkvtest.SequentialEntriesSeq(100, 10)
	table, err := tw.Write(entries)
	require.NoError(t, err)

	data := table.Document()
	table = sst.NewTableFromDocument(fs, data)

	// Prove that startKey and endKey were set
	assert.True(t, table.RangeContainsKey([]byte("099")), "contains key in sequence")
	assert.False(t, table.RangeContainsKey([]byte("100")), "does not contain key after sequence")

	assert.Greater(t, table.Size(), int64(0), "size was set")

	// Get a key that we know isn't there to test bloom filter
	filterHits := metrics.GetOrCreateCounter("table_filter_hit")
	_, err = table.Get([]byte("not-there"))
	assert.ErrorIs(t, err, kv.ErrNotFound)
	assert.Equal(t, filterHits.Get(), uint64(1), "added one filter hit to counter")

	// Get a key that is there is a search index.
	v, err := table.Get([]byte("099"))
	require.NoError(t, err)
	assert.Equal(t, filterHits.Get(), uint64(1), "no filter hit added")
	assert.Equal(t, v.Value(), []byte("099"))

	assert.Equal(t, uint64(10), table.Age(), "loaded table maintains age")
}
