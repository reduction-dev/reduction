package dkv_test

import (
	"bytes"
	"os"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"reduction.dev/reduction/dkv"
	"reduction.dev/reduction/dkv/dkvtest"
	"reduction.dev/reduction/dkv/kv"
	"reduction.dev/reduction/dkv/storage"
	"reduction.dev/reduction/storage/objstore"
	"reduction.dev/reduction/util/size"
	"reduction.dev/reduction/util/sliceu"
)

func BenchmarkLoad_PutAndGet(b *testing.B) {
	fs := storage.NewLocalFilesystem(b.TempDir())
	db := dkv.Open(dkv.DBOptions{FileSystem: fs, MemTableSize: 8 * size.MB}, nil)

	putEntries := slices.Collect(dkvtest.RandomEntriesSeq(800_000))
	getEntries := sliceu.Sample(putEntries, 800_000)

	b.ResetTimer()
	for i, putEntry := range putEntries {
		db.Put(putEntry.Key(), putEntry.Value())

		gotEntry, err := db.Get(getEntries[i].Key())
		if err != nil {
			if err == kv.ErrNotFound {
				continue
			}
			require.NoError(b, db.WaitOnTasks())
		}
		if !bytes.Equal(gotEntry.Value(), getEntries[i].Value()) {
			b.Fatalf("value mismatch: %v, %v, at index %d", getEntries[i].Value(), gotEntry.Value(), i)
		}
	}
	b.StopTimer()
	require.NoError(b, db.WaitOnTasks())
}

func TestLoad_LocalFS_PutAndScan(t *testing.T) {
	if os.Getenv("DKV_SLOW") == "" {
		t.Skip("Skipping slow load test")
	}
	db := dkv.Open(dkv.DBOptions{
		FileSystem:     storage.NewLocalFilesystem(t.TempDir()),
		MemTableSize:   10 * size.KB,
		TargetFileSize: 20 * size.KB,
	}, nil)

	// We need random entries with no duplicates to make counting the number of
	// expected entries for a prefix easier.
	putEntries := dkvtest.ShuffledSequentialEntries(5_000)
	putPrefixCounts := make(map[string]int, 1_000)

	for i, putEntry := range putEntries {
		db.Put(putEntry.Key(), putEntry.Value())

		// As we write, keep track of a 3 char prefix
		putPrefix := putEntry.Key()[:3]
		putPrefixCounts[string(putPrefix)] += 1

		scanPrefix := sliceu.SampleOne(putEntries).Key()[:3]
		var scanErr error
		foundEntries := slices.Collect(db.ScanPrefix(scanPrefix, &scanErr))
		require.NoError(t, scanErr)

		// Should find the same number of entries that were written with this prefix (if any).
		require.Len(t, foundEntries, putPrefixCounts[string(scanPrefix)])
		t.Log("records written: ", i+1)
		t.Log(db.Diagnostics())
	}
	require.NoError(t, db.WaitOnTasks())
}

func TestLoad_S3_PutAndGet(t *testing.T) {
	if os.Getenv("DKV_SLOW") == "" {
		t.Skip("Skipping slow load test")
	}

	s3Service := objstore.NewMemoryS3Service()
	fs := storage.NewS3FileSystem(s3Service, "bucket")
	db := dkv.Open(dkv.DBOptions{
		FileSystem:     fs,
		MemTableSize:   1 * size.MB,
		TargetFileSize: 2 * size.MB,
	}, nil)

	putEntries := slices.Collect(dkvtest.RandomEntriesSeq(800_000))
	getEntries := sliceu.Sample(putEntries, 800_000)

	for i, putEntry := range putEntries {
		db.Put(putEntry.Key(), putEntry.Value())

		gotEntry, err := db.Get(getEntries[i].Key())
		if err != nil {
			if err == kv.ErrNotFound {
				continue
			}
			require.NoError(t, db.WaitOnTasks())
		}
		if !bytes.Equal(gotEntry.Value(), getEntries[i].Value()) {
			t.Fatalf("value mismatch: %v, %v, at index %d", getEntries[i].Value(), gotEntry.Value(), i)
		}
		t.Log("records written: ", i+1)
		t.Log(db.Diagnostics())
	}
	t.Log("Cost: ", fs.USDCost())
	require.NoError(t, db.WaitOnTasks())
}
