// A package of test helpers
package dkvtest

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"iter"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strconv"

	"github.com/stretchr/testify/require"
	"reduction.dev/reduction/dkv/kv"
	"reduction.dev/reduction/dkv/sst"
)

type testingT interface {
	Errorf(format string, args ...any)
	FailNow()
	Helper()
}

// Assert that two kv.Entry values are equal despite being different concrete
// types.
func EntryEqual(t testingT, want, got kv.Entry) {
	t.Helper()
	if !bytes.Equal(want.Value(), got.Value()) ||
		!bytes.Equal(want.Key(), got.Key()) ||
		want.IsDelete() != got.IsDelete() {

		t.Errorf(
			`Entries not equivalent.
Want {Key: %s, Value: %s, IsDeleted: %t}
Got {Key: %s, Value: %s, IsDeleted: %t}`,
			want.Key(), want.Value(), want.IsDelete(),
			got.Key(), got.Value(), got.IsDelete(),
		)
	}
}

func EntriesEqual[T1 kv.Entry, T2 kv.Entry](t testingT, want []T1, got []T2) {
	t.Helper()
	if len(want) != len(got) {
		t.Errorf(
			`Entries not the same length:
Want len %d
Got len %d`,
			len(want), len(got))
	}

	longestCount := max(len(want), len(got))
	for i := range longestCount {
		var wantEntry kv.Entry
		if i < len(want) {
			wantEntry = want[i]
		}
		var gotEntry kv.Entry
		if i < len(got) {
			gotEntry = got[i]
		}
		if wantEntry != nil && gotEntry != nil {
			EntryEqual(t, wantEntry, gotEntry)
		} else if wantEntry != nil {
			t.Errorf("Index %d, Want {Key: %s, Value: %s, IsDeleted: %t}, Got <missing>", i, wantEntry.Key(), wantEntry.Value(), wantEntry.IsDelete())
		} else {
			t.Errorf("Index %d, Want <missing>, Got {Key: %s, Value: %s, IsDeleted: %t}", i, gotEntry.Key(), gotEntry.Value(), gotEntry.IsDelete())
		}
	}
}

// A concrete kv.Entry type to use in tests for assertions.
type TestEntry struct {
	key      string
	value    []byte
	seqNum   uint64
	isDelete bool
}

func NewKVEntry(key string, value string) *TestEntry {
	return &TestEntry{key: key, value: []byte(value)}
}

func NewDeleteEntry(key string) *TestEntry {
	return &TestEntry{key: key, isDelete: true}
}

func (t *TestEntry) IsDelete() bool {
	return t.isDelete
}

func (t *TestEntry) Key() []byte {
	return []byte(t.key)
}

func (t *TestEntry) Value() []byte {
	return []byte(t.value)
}

func (t *TestEntry) SeqNum() uint64 {
	return t.seqNum
}

var _ kv.Entry = (*TestEntry)(nil)

type EntryList struct {
	Entries []kv.Entry
	Size    int
}

func RandomEntriesList(length int) *EntryList {
	sizeOfAllEntries := 0
	entries := make([]kv.Entry, 0, length)
	for e := range RandomEntriesSeq(length) {
		entries = append(entries, e)
		sizeOfAllEntries += int(sst.FlushSize(e))
	}

	return &EntryList{
		Entries: entries,
		Size:    sizeOfAllEntries,
	}
}

func RandomEntriesSeq(length int) iter.Seq[kv.Entry] {
	return func(yield func(kv.Entry) bool) {
		for range length {
			iValue := fmt.Appendf(nil, "%010d", rand.Uint32())
			if !yield(NewKVEntry(string(iValue), string(iValue))) {
				return
			}
		}
	}
}

func SequentialEntriesList(length int) *EntryList {
	sizeOfAllEntries := 0
	entries := make([]kv.Entry, 0, length)
	for e := range SequentialEntriesSeq(length, 0) {
		entries = append(entries, e)
		sizeOfAllEntries += int(sst.FlushSize(e))
	}

	return &EntryList{
		Entries: entries,
		Size:    sizeOfAllEntries,
	}
}

func SequentialEntriesSeq(length int, seqNum uint64) iter.Seq[kv.Entry] {
	valueLen := len(strconv.Itoa(length))
	return func(yield func(kv.Entry) bool) {
		for i := range length {
			iValue := fmt.Appendf(nil, "%0*d", valueLen, i)
			if !yield(&TestEntry{
				key:    string(iValue),
				value:  iValue,
				seqNum: uint64(i) + seqNum,
			}) {
				return
			}
		}
	}
}

func ShuffledSequentialEntries(length int) []kv.Entry {
	entries := slices.Collect(SequentialEntriesSeq(length, 0))
	rand.Shuffle(len(entries), func(i, j int) {
		entries[i], entries[j] = entries[j], entries[i]
	})
	return entries
}

func (l *EntryList) At(index int) kv.Entry {
	return l.Entries[index]
}

func SSTFileCount(t testingT, dir string) int {
	dbFiles, err := os.ReadDir(dir)
	require.NoError(t, err)
	sstFileCount := 0
	for _, dirEntry := range dbFiles {
		if filepath.Ext(dirEntry.Name()) == ".sst" {
			sstFileCount++
		}
	}
	return sstFileCount
}

type KV struct {
	Key   []byte
	Value []byte
}

type SequenceGenerator struct {
	NextValue uint64
}

func (s *SequenceGenerator) All() iter.Seq2[uint64, KV] {
	return func(yield func(uint64, KV) bool) {
		for {
			value := make([]byte, 8)
			numValue := s.NextValue
			s.NextValue++
			binary.BigEndian.PutUint64(value, numValue)
			if !yield(numValue, KV{Key: value, Value: value}) {
				return
			}
		}
	}
}
