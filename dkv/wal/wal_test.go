package wal_test

import (
	"errors"
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
	w := wal.NewWriter(fs, 0)

	entries := slices.Collect(dkvtest.SequentialEntriesSeq(10, 0))
	firstHalf, secondHalf := entries[:5], entries[5:]

	for _, e := range firstHalf {
		w.Put(e.Key(), e.Value(), e.SeqNum())
	}

	firstHalfSeqNum := len(firstHalf) - 1

	// When saving the checkpoint it will have the latest seqNum written to SSTables.
	// Given the WAL number and that seqNum from the checkpoint
	// Playback from the WAL can skip entries earlier than the checkpoint's seqNum
	// And only replay entries after the checkpoint's seqNum.

	for _, e := range secondHalf {
		w.Put(e.Key(), e.Value(), e.SeqNum())
	}

	require.NoError(t, w.Save())

	r := wal.NewReader(fs, w.Handle(uint64(firstHalfSeqNum)))
	replayedEntries, errs := iteru.Collect2(r.All())
	require.NoError(t, errors.Join(errs...))
	dkvtest.EntriesEqual(t, secondHalf, replayedEntries)
}

func TestReplayEmptyFile(t *testing.T) {
	fs := storage.NewMemoryFilesystem()
	w := wal.NewWriter(fs, 0)
	require.NoError(t, w.Save())

	r := wal.NewReader(fs, w.Handle(0))
	replayedEntries, errs := iteru.Collect2(r.All())
	require.NoError(t, errors.Join(errs...))
	assert.Len(t, replayedEntries, 0)
}

func TestTruncate(t *testing.T) {
	fs := storage.NewMemoryFilesystem()
	w := wal.NewWriter(fs, 0)

	putEntries := slices.Collect(dkvtest.SequentialEntriesSeq(10, 1))
	for chunk := range slices.Chunk(putEntries, 3) {
		for _, e := range chunk {
			w.Put(e.Key(), e.Value(), e.SeqNum())
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
	w := wal.NewWriter(fs, 0)

	entries := slices.Collect(dkvtest.SequentialEntriesSeq(10, 1))
	firstHalf, secondHalf := entries[:5], entries[5:]

	for _, e := range firstHalf {
		t.Log("firstHalf: ", e)
		w.Put(e.Key(), e.Value(), e.SeqNum())
	}

	wNext := w.Rotate(fs)

	for _, e := range secondHalf {
		wNext.Put(e.Key(), e.Value(), e.SeqNum())
	}

	require.NoError(t, wNext.Save())

	r := wal.NewReader(fs, wNext.Handle(0)) // 0 in the case no SSTs have been written
	replayedEntries, errs := iteru.Collect2(r.All())
	require.NoError(t, errors.Join(errs...))
	dkvtest.EntriesEqual(t, entries, replayedEntries)
}
