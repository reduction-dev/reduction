package wal_test

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"reduction.dev/reduction/dkv/dkvtest"
	"reduction.dev/reduction/dkv/kv"
	"reduction.dev/reduction/dkv/storage"
	"reduction.dev/reduction/dkv/wal"
	"reduction.dev/reduction/util/iteru"
)

func TestReplay(t *testing.T) {
	fs := storage.NewMemoryFilesystem()
	w := wal.NewWriter(fs, 0, 1024) // Use large enough size to avoid hitting limit

	entries := slices.Collect(dkvtest.SequentialEntriesSeq(10, 0))
	firstHalf, secondHalf := entries[:5], entries[5:]

	for _, e := range firstHalf {
		full := w.Put(e.Key(), e.Value(), e.SeqNum())
		assert.False(t, full)
	}

	firstHalfSeqNum := len(firstHalf) - 1

	// When saving the checkpoint it will have the latest seqNum written to SSTables.
	// Given the WAL number and that seqNum from the checkpoint
	// Playback from the WAL can skip entries earlier than the checkpoint's seqNum
	// And only replay entries after the checkpoint's seqNum.

	for _, e := range secondHalf {
		full := w.Put(e.Key(), e.Value(), e.SeqNum())
		assert.False(t, full)
	}

	require.NoError(t, w.Save())

	r := wal.NewReader(fs, w.Handle(uint64(firstHalfSeqNum)))
	replayedEntries, errs := iteru.Collect2(r.All())
	require.NoError(t, errors.Join(errs...))
	dkvtest.EntriesEqual(t, secondHalf, replayedEntries)
}

func TestReplayEmptyFile(t *testing.T) {
	fs := storage.NewMemoryFilesystem()
	w := wal.NewWriter(fs, 0, 1024)
	require.NoError(t, w.Save())

	r := wal.NewReader(fs, w.Handle(0))
	replayedEntries, errs := iteru.Collect2(r.All())
	require.NoError(t, errors.Join(errs...))
	assert.Len(t, replayedEntries, 0)
}

func TestTruncate(t *testing.T) {
	fs := storage.NewMemoryFilesystem()
	w := wal.NewWriter(fs, 0, 1024)

	putEntries := slices.Collect(dkvtest.SequentialEntriesSeq(10, 1))
	for chunk := range slices.Chunk(putEntries, 3) {
		for _, e := range chunk {
			full := w.Put(e.Key(), e.Value(), e.SeqNum())
			assert.False(t, full)
		}
		w.Cut()
	}

	w.Truncate(5)
	require.NoError(t, w.Save())

	// Panic because requesting to start reading after seqNumber 2 but first seq
	// number is 4.
	assert.Panics(t, func() {
		r := wal.NewReader(fs, w.Handle(2))
		iteru.Collect2(r.All())
	})

	r := wal.NewReader(fs, w.Handle(3))
	walEntries, err := iteru.Collect2(r.All())
	require.NoError(t, errors.Join(err...))
	t.Log(walEntries)
	dkvtest.EntriesEqual(t, []kv.Entry{
		dkvtest.NewKVEntry("03", "03"),
		dkvtest.NewKVEntry("04", "04"),
		dkvtest.NewKVEntry("05", "05"),
		dkvtest.NewKVEntry("06", "06"),
		dkvtest.NewKVEntry("07", "07"),
		dkvtest.NewKVEntry("08", "08"),
		dkvtest.NewKVEntry("09", "09"),
	}, walEntries)
}

// When rotating the wal writer, the next writer includes all the buffers from
// the previous writer so that, in the case that no SSTs are written, the latest
// WAL will contain _all_ the records ever written. Only wal.Truncate removes
// data.
func TestRotateWithNoTruncation(t *testing.T) {
	fs := storage.NewMemoryFilesystem()
	w := wal.NewWriter(fs, 0, 1024)

	entries := slices.Collect(dkvtest.SequentialEntriesSeq(10, 1))
	firstHalf, secondHalf := entries[:5], entries[5:]

	for _, e := range firstHalf {
		t.Log("firstHalf: ", e)
		full := w.Put(e.Key(), e.Value(), e.SeqNum())
		assert.False(t, full)
	}

	wNext := w.Rotate(fs)

	for _, e := range secondHalf {
		full := wNext.Put(e.Key(), e.Value(), e.SeqNum())
		assert.False(t, full)
	}

	require.NoError(t, wNext.Save())

	r := wal.NewReader(fs, wNext.Handle(0)) // 0 in the case no SSTs have been written
	replayedEntries, errs := iteru.Collect2(r.All())
	require.NoError(t, errors.Join(errs...))
	dkvtest.EntriesEqual(t, entries, replayedEntries)
}

func TestSizeLimit(t *testing.T) {
	fs := storage.NewMemoryFilesystem()
	// Use a small size limit that will be hit after a few entries
	w := wal.NewWriter(fs, 0, 10)

	entries := slices.Collect(dkvtest.SequentialEntriesSeq(5, 0))
	var hitLimit bool

	// Write until we hit the size limit
	for _, e := range entries {
		full := w.Put(e.Key(), e.Value(), e.SeqNum())
		if full {
			hitLimit = true
			break
		}
	}
	assert.True(t, hitLimit, "expected to hit size limit in first WAL")

	// Rotate and try again with new entries
	w = w.Rotate(fs)
	hitLimit = false

	entries = slices.Collect(dkvtest.SequentialEntriesSeq(5, 5))
	for _, e := range entries {
		full := w.Put(e.Key(), e.Value(), e.SeqNum())
		if full {
			hitLimit = true
			break
		}
	}
	assert.True(t, hitLimit, "expected to hit size limit in rotated WAL")
}

func TestWALFullWorkflow(t *testing.T) {
	fs := storage.NewMemoryFilesystem()
	// Use a small size limit that will be hit after a few entries
	w := wal.NewWriter(fs, 0, 30)

	entries := slices.Collect(dkvtest.SequentialEntriesSeq(10, 0))
	var hitLimit bool
	var lastSeqNum uint64

	// Write until we hit size limit
	for _, e := range entries[:5] {
		full := w.Put(e.Key(), e.Value(), e.SeqNum())
		lastSeqNum = e.SeqNum()
		if full {
			hitLimit = true
			break
		}
	}
	assert.True(t, hitLimit, "expected to hit size limit")

	// When WAL is full, we Cut() it to segment the entries
	// This creates a new empty activeBuffer, so WAL should not be full anymore
	w.Cut()

	// Write more entries
	full := w.Put([]byte("test"), []byte("value"), lastSeqNum+1)
	assert.False(t, full, "WAL should have more space in active buffer after cut")

	// Simulate SST flush by truncating up to latest seq num
	w.Truncate(lastSeqNum)
	require.NoError(t, w.Save())

	// Verify we can read back the entries after the truncation point
	r := wal.NewReader(fs, w.Handle(lastSeqNum))
	walEntries, err := iteru.Collect2(r.All())
	require.NoError(t, errors.Join(err...))
	assert.Len(t, walEntries, 1, "Should only have entries after truncation point")
}

func TestWALSizeDecreasesAfterMemtableRotationAndCheckpoint(t *testing.T) {
	fs := storage.NewMemoryFilesystem()
	w := wal.NewWriter(fs, 0, 1024)

	// Write data
	for i := range 100 {
		seqNum := uint64(i + 1)
		w.Put(fmt.Appendf(nil, "key%d", i), fmt.Appendf(nil, "value%d", i), seqNum)

		// Simulate the sync phase of rotating memtables
		if seqNum == 75 {
			w.Cut()
		}
	}

	// In the async phase of rotating memtables we truncate the WAL to the sequence
	// number of the last entry written to the SSTable.
	w.Truncate(75)

	// During checkpointing, the sync phase rotates the WAL to start a new file
	w2 := w.Rotate(fs)

	// And in the async phase we persist the previous WAL.
	require.NoError(t, w.Save())

	// Get the size of the first WAL file
	filePaths := fs.List()
	assert.Equal(t, []string{"000000.wal"}, filePaths, "wrote one WAL file")
	firstFileContent, err := storage.ReadAll(fs.Open("000000.wal"))
	require.NoError(t, err)
	t.Logf("Size of 1st WAL file: %d bytes", len(firstFileContent))

	// Start again. Sync phase of memtable rotation cuts at latest seq num
	w2.Cut()

	// Async phase of memtable rotation truncates
	w2.Truncate(100)

	// Sync phase of checkpointing rotates to start a new file
	_ = w2.Rotate(fs)

	// Async phase of checkpointing saves the previous WAL
	require.NoError(t, w2.Save())

	// Get the size of the second WAL file
	filePaths = fs.List()
	assert.Equal(t, []string{"000000.wal", "000001.wal"}, filePaths, "wrote second WAL file")
	secondFileContents, err := storage.ReadAll(fs.Open("000001.wal"))
	require.NoError(t, err)
	t.Logf("Size of 2nd WAL file: %d bytes", len(secondFileContents))

	assert.Less(t, len(secondFileContents), len(firstFileContent), "WAL file size should decrease after truncation")
}
