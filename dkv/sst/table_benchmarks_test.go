package sst_test

import (
	"crypto/rand"
	"slices"
	"testing"

	"golang.org/x/text/language"
	"golang.org/x/text/message"

	"github.com/stretchr/testify/require"
	"reduction.dev/reduction/dkv/dkvtest"
	"reduction.dev/reduction/dkv/sst"
	"reduction.dev/reduction/dkv/storage"
	"reduction.dev/reduction/util/sliceu"
)

func BenchmarkUsingBinarySearchIndexForGet_MemoryFile(b *testing.B) {
	tw := sst.NewTableWriter(storage.NewMemoryFilesystem(), 0)
	p := message.NewPrinter(language.English)

	runEntryCounts := []int{
		100_000,
		500_000,   // 500MB file with 1KB records
		2_500_000, // 500MB file with 200B records
	}

	for _, entryCount := range runEntryCounts {
		entries := dkvtest.SequentialEntriesSeq(entryCount, 0)
		table, err := tw.Write(entries)
		require.NoError(b, err)

		b.Run(p.Sprintf("get in %d entries", entryCount), func(b *testing.B) {
			pickedEntryKey := sliceu.Sample(slices.Collect(entries), 1)[0].Key()

			b.ResetTimer()
			_, _ = table.Get(pickedEntryKey)
		})
	}
}

func BenchmarkUsingBinarySearchIndexForGet_DiskFile(b *testing.B) {
	tw := sst.NewTableWriter(storage.NewMemoryFilesystem(), 0)
	p := message.NewPrinter(language.English)

	runEntryCounts := []int{
		100_000,
		500_000,   // 500MB file with 1KB records
		2_500_000, // 500MB file with 200B records
	}

	for _, entryCount := range runEntryCounts {
		entries := dkvtest.SequentialEntriesSeq(entryCount, 0)
		table, err := tw.Write(entries)
		require.NoError(b, err)

		b.Run(p.Sprintf("get in %d entries", entryCount), func(b *testing.B) {
			pickedEntryKey := sliceu.Sample(slices.Collect(entries), 1)[0].Key()

			b.ResetTimer()
			_, _ = table.Get(pickedEntryKey)
		})
	}
}

func BenchmarkGetNotFound(b *testing.B) {
	fs := storage.NewLocalFilesystem(b.TempDir())
	tw := sst.NewTableWriter(fs, 0)
	p := message.NewPrinter(language.English)

	runEntryCounts := []int{
		100_000,
		500_000,   // 500MB file with 1KB records
		2_500_000, // 500MB file with 200B records
	}

	for _, entryCount := range runEntryCounts {
		entries := dkvtest.SequentialEntriesSeq(entryCount, 0)
		table, err := tw.Write(entries)
		require.NoError(b, err)

		b.Run(p.Sprintf("get in %d entries", entryCount), func(b *testing.B) {
			missingKey := make([]byte, 10)
			_, err = rand.Read(missingKey)
			require.NoError(b, err)
			missingKey = append([]byte("not-found"), missingKey...)

			b.ResetTimer()
			_, _ = table.Get(missingKey)
		})
	}
}
