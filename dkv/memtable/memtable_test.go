package memtable_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"reduction.dev/reduction/dkv/dkvtest"
	"reduction.dev/reduction/dkv/memtable"
	"reduction.dev/reduction/dkv/sst"
	"reduction.dev/reduction/util/size"
	"reduction.dev/reduction/util/sliceu"
)

func TestMemtableSizeAdjustment(t *testing.T) {
	table := memtable.NewMemTable(32 * size.MB)

	table.Put([]byte("key"), []byte("value"), 0)
	assert.Equal(t, 25, table.Size(), "one entry increases size")

	table.Put([]byte("key"), []byte(""), 0)
	assert.Equal(t, 20, table.Size(), "replacing with smaller entry decreases size")

	table.Put([]byte("key2"), []byte("value"), 0)
	assert.Equal(t, 46, table.Size(), "adding another key increase size")
}

func TestMemtableFull(t *testing.T) {
	entry := dkvtest.NewKVEntry("key", "value")
	table := memtable.NewMemTable(uint64(sst.FlushSize(entry)))

	full := table.Put(entry.Key(), entry.Value(), 0)
	assert.False(t, full)

	full = table.Put([]byte("key2"), []byte("value"), 0)
	assert.True(t, full)
}

func BenchmarkMemTable_PutAndScan(b *testing.B) {
	table := memtable.NewMemTable(32 * size.MB)

	putEntries := dkvtest.RandomEntriesList(800_000)
	putPrefixCounts := make(map[string]int, 1_000)

	for _, putEntry := range putEntries.Entries {
		table.Put(putEntry.Key(), putEntry.Value(), putEntry.SeqNum())

		// As we write, keep track of a 3 char prefix
		putPrefix := putEntry.Key()[:3]
		putPrefixCounts[string(putPrefix)] += 1
	}

	// Make a list of prefixes from the written entries
	samples := sliceu.Sample(putEntries.Entries, 10_000)
	prefixes := make([][]byte, len(samples))
	for i, s := range samples {
		prefixes[i] = s.Key()[:3]
	}

	for i := 0; b.Loop(); i++ {
		it := table.ScanPrefix(prefixes[i%len(prefixes)])
		for range it {
			// Consume the iterator
		}
	}
}
