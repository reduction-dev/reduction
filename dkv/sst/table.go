package sst

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"runtime"
	"runtime/debug"
	"strings"

	"github.com/VictoriaMetrics/metrics"
	"reduction.dev/reduction/dkv/bloom"
	"reduction.dev/reduction/dkv/fields"
	"reduction.dev/reduction/dkv/kv"
	"reduction.dev/reduction/dkv/storage"
	"reduction.dev/reduction/util/size"
)

var (
	filterHit  = metrics.NewCounter("table_filter_hit")
	filterMiss = metrics.NewCounter("table_filter_miss")
	filterFP   = metrics.NewCounter("table_filter_fp")
)

func ResetMetrics() {
	filterHit.Set(0)
	filterMiss.Set(0)
	filterFP.Set(0)
}

type Table struct {
	file           storage.File
	size           int64
	entriesSize    int64
	searchIndex    *SearchIndex
	filter         *bloom.Filter
	startKey       []byte
	endKey         []byte
	startSeqNum    uint64
	endSeqNum      uint64
	metadataLoaded bool
}

// NewTable initializes a new, empty table
func NewTable(file storage.File) *Table {
	t := &Table{
		file:        file,
		searchIndex: &SearchIndex{},
		filter:      bloom.NewFilter(32*size.KB, 5),
		size:        0,
	}

	deleteFunc := file.CreateDeleteFunc()
	runtime.AddCleanup(t, func(f func() error) {
		if err := f(); err != nil {
			slog.Error("table cleanup", "err", err)
		}
	}, deleteFunc)

	return t
}

type TableDocument struct {
	StartKey    string
	EndKey      string
	Size        uint64
	EntriesSize uint64
	URI         string
	StartSeqNum uint64
	EndSeqNum   uint64
}

// TODO: New parameter for canDeleteTable
func NewTableFromDocument(fs storage.FileSystem, doc TableDocument) *Table {
	return &Table{
		file:        fs.Open(doc.URI),
		size:        int64(doc.Size),
		entriesSize: int64(doc.EntriesSize),
		startKey:    []byte(doc.StartKey),
		endKey:      []byte(doc.EndKey),
		startSeqNum: doc.StartSeqNum,
		endSeqNum:   doc.EndSeqNum,
	}
}

func (t *Table) Get(key []byte) (kv.Entry, error) {
	t.ensureMetadataLoaded()
	if !t.filter.MightHave(key) {
		filterHit.Inc()
		return nil, kv.ErrNotFound
	}
	filterMiss.Inc()
	cur := storage.NewBoundedCursor(t.file, 0, uint64(t.entriesSize))

	// Search for offset range of a key in the index.
	start, end, err := t.searchIndex.Search(key, func(offset int64) ([]byte, error) {
		cur.Move(offset)

		// Read the key at the offset
		b, err := fields.ReadVarBytes(cur)
		if err != nil {
			return nil, fmt.Errorf("reading bytes at %d in %s: %v", offset, t.file.Name(), err)
		}

		return b, nil
	})
	if err != nil {
		return nil, fmt.Errorf("table searching: %w", err)
	}

	// Scan for the key between the start and end offsets provided
	cur.Move(start)
	for cur.Offset() < end {
		currentKey, err := fields.ReadVarBytes(cur)
		if errors.Is(err, io.EOF) {
			filterFP.Inc()
			return nil, kv.ErrNotFound
		}
		if err != nil {
			return nil, err
		}

		seqNum, err := fields.ReadUint64(cur)
		if err != nil {
			return nil, err
		}

		wasDeleted, err := fields.ReadTombstone(cur)
		if err != nil {
			return nil, fmt.Errorf("SST reading tombstone: %w", err)
		}

		if wasDeleted {
			// If the entry we want was deleted, there's no value to read
			if bytes.Equal(currentKey, key) {
				return &Entry{currentKey, nil, seqNum, true}, nil
			}
			// When we see a deleted entry, we can just skip to the next entry
			continue
		}

		// If this isn't the key we want, skip to next record.
		if !bytes.Equal(currentKey, key) {
			if err := fields.SkipVarBytes(cur); err != nil {
				return nil, fmt.Errorf("SST skipping to next record: %w, trace: %s", err, debug.Stack())
			}
			continue
		}

		// It is the key so read the value
		value, err := fields.ReadVarBytes(cur)
		if err != nil {
			return nil, fmt.Errorf("SST read value: %w", err)
		}
		return &Entry{currentKey, value, seqNum, false}, nil
	}

	filterFP.Inc()
	return nil, kv.ErrNotFound
}

func (t *Table) ScanPrefix(prefix []byte, errOut *error) iter.Seq[kv.Entry] {
	t.ensureMetadataLoaded()
	cur := storage.NewBoundedCursor(t.file, 0, uint64(t.entriesSize))

	return func(yield func(kv.Entry) bool) {
		for {
			key, err := fields.ReadVarBytes(cur)
			if errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				*errOut = fmt.Errorf("ScanPrefix reading key from %s: %v", t.file.Name(), err)
				return
			}

			// Skip if key doesn't have the prefix
			if !bytes.HasPrefix(key, prefix) {
				if err := fields.SkipUint64(cur); err != nil {
					*errOut = err
					return
				}
				wasDeleted, err := fields.ReadTombstone(cur)
				if err != nil {
					*errOut = err
					return
				}
				if !wasDeleted {
					if err := fields.SkipVarBytes(cur); err != nil {
						*errOut = err
						return
					}
				}
				continue
			}

			seqNum, err := fields.ReadUint64(cur)
			if err != nil {
				*errOut = err
				return
			}

			// If this record was deleted, yield a deleted entry
			wasDeleted, err := fields.ReadTombstone(cur)
			if err != nil {
				*errOut = err
				return
			}
			if wasDeleted {
				if !yield(&Entry{key: key, isDelete: true, seqNum: seqNum}) {
					return
				}
				continue
			}

			value, err := fields.ReadVarBytes(cur)
			if err != nil {
				*errOut = err
				return
			}
			if !yield(&Entry{key: key, value: value, seqNum: seqNum}) {
				return
			}
		}
	}
}

// Determine if the table may contain a key based on the start and end keys.
func (t *Table) RangeContainsKey(key []byte) bool {
	return bytes.Compare(t.startKey, key) < 1 && bytes.Compare(t.endKey, key) > -1
}

// Determine if the table may contain a key prefix based on the start and end keys.
func (t *Table) RangeContainsPrefix(prefix []byte) bool {
	return (bytes.Compare(t.startKey, prefix) < 1 && bytes.Compare(t.endKey, prefix) > -1) ||
		bytes.HasPrefix(t.startKey, prefix) ||
		bytes.HasPrefix(t.endKey, prefix)
}

// Implements a cmp.Compare func for the table's range and a provided key.
func (t *Table) RangeKeyCompare(key []byte) int {
	if bytes.Compare(t.startKey, key) == 1 {
		return 1
	}
	if bytes.Compare(t.endKey, key) == -1 {
		return -1
	}
	return 0
}

// Implements a cmp.Compare func for the table's range and a provided prefix.
func (t *Table) RangePrefixCompare(prefix []byte) int {
	if bytes.HasPrefix(t.startKey, prefix) || bytes.HasPrefix(t.endKey, prefix) {
		return 0
	}
	if bytes.Compare(t.startKey, prefix) == 1 {
		return 1
	}
	if bytes.Compare(t.endKey, prefix) == -1 {
		return -1
	}
	return 0
}

func (t *Table) Size() int64 {
	return t.size
}

func (t *Table) EntriesSize() int64 {
	return t.entriesSize
}

func (t *Table) Age() uint64 {
	return t.startSeqNum
}

func (t *Table) Name() string {
	return t.file.Name()
}

func (t *Table) URI() string {
	return t.file.URI()
}

func (t *Table) Diagnostics() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("file: %s", t.file.Name()))
	sb.WriteString(fmt.Sprintf("\nsize: %d", t.size))
	sb.WriteString("\nfirst 10 rows:")
	var scanErr error
	count := 0
	for entry := range t.ScanPrefix(nil, &scanErr) {
		if count > 10 {
			break
		}
		sb.WriteString(fmt.Sprintf("\n %s : %s", string(entry.Key()), string(entry.Value())))
		count++
	}
	return sb.String()
}

func (t *Table) ensureMetadataLoaded() {
	if t.metadataLoaded {
		return
	}
	t.loadFooter()
	t.metadataLoaded = true
}

func (t *Table) Document() TableDocument {
	return TableDocument{
		StartKey:    string(t.startKey),
		EndKey:      string(t.endKey),
		Size:        uint64(t.size),
		EntriesSize: uint64(t.entriesSize),
		URI:         t.file.URI(),
		StartSeqNum: t.startSeqNum,
		EndSeqNum:   t.endSeqNum,
	}
}

func OrderOldToNew(a, b *Table) int {
	return cmp.Compare(a.Age(), b.Age())
}
