package memtable

import (
	"fmt"
	"iter"
	"strings"
	"sync"

	"reduction.dev/reduction/dkv/kv"
)

// This list's metaphor is a thread-safe queue, however the slices of tables is
// immutably replaced when changed. New tables are enqueued and older tables are
// dequeued as they are flushed. The process of adding a new memtable must be
// synchronous so that after each Put, Get works. The process of flushing
// memtables is asynchronous.
type List struct {
	// The last table is the active table and the rest are immutable (sealed).
	tables             []*MemTable
	tablesMu           *sync.RWMutex
	targetMemTableSize uint64
}

type MemTableOptions struct {
	MemSize   uint64
	NumLevels int
}

// NewList creates a new queue of memtables with one active memtable.
func NewList(memtableOptions *MemTableOptions) *List {
	tables := make([]*MemTable, 1, memtableOptions.NumLevels)
	tables[0] = NewMemTable(memtableOptions.MemSize)

	return &List{
		tables:             tables,
		tablesMu:           &sync.RWMutex{},
		targetMemTableSize: memtableOptions.MemSize,
	}
}

func (l *List) Sealed() []*MemTable {
	tables := l.tablesSnap()
	return tables[:len(tables)-1]
}

// Remove memtables from the tail of the queue. This method checks that tables
// being removed are at the end of the queue and in order. This ensures the
// caller obtains sealed tables and dequeues them serially.
func (l *List) Dequeue(tables []*MemTable) {
	l.tablesMu.Lock()
	defer l.tablesMu.Unlock()
	for i, t := range tables {
		if l.tables[i] != t {
			panic("BUG: dequeued table not found at the correct location in memtable queue")
		}
	}
	l.tables = l.tables[len(tables):]
}

func (l *List) Rotate() {
	l.tablesMu.Lock()
	defer l.tablesMu.Unlock()
	l.tables = append(l.tables[:], NewMemTable(l.targetMemTableSize))
}

func (l *List) Delete(key []byte, seqNum uint64) (full bool) {
	return l.active().Delete(key, seqNum)
}

func (l *List) Put(key []byte, value []byte, seqNum uint64) (full bool) {
	return l.active().Put(key, value, seqNum)
}

func (l *List) Get(key []byte) (kv.Entry, error) {
	for _, t := range l.tablesSnap() {
		v, err := t.Get(key)
		if err != nil {
			if err == kv.ErrNotFound {
				continue
			}
			return nil, fmt.Errorf("table %#v, %w", t, err)
		}
		return v, nil
	}
	return nil, kv.ErrNotFound
}

func (l *List) ScanPrefix(prefix []byte, errOut *error) iter.Seq[kv.Entry] {
	tables := l.tablesSnap()
	iters := make([]iter.Seq[kv.Entry], len(tables))
	for i, table := range tables {
		iters[i] = table.ScanPrefix(prefix)
	}
	return kv.MergeEntries(iters)
}

func (l *List) Diagnostics() string {
	var sb strings.Builder

	tables := l.tablesSnap()
	sb.WriteString(fmt.Sprintf("\nMemTables (num: %d)", len(tables)))
	for i, t := range tables {
		sb.WriteString(fmt.Sprintf("\n %d: size %d", i, t.Size()))
	}

	return sb.String()
}

// tablesSnap returns the current
func (l *List) tablesSnap() []*MemTable {
	l.tablesMu.RLock()
	defer l.tablesMu.RUnlock()
	return l.tables
}

func (l *List) active() *MemTable {
	tables := l.tablesSnap()
	if len(tables) == 0 {
		panic("should always have at least one memTable")
	}
	return tables[len(tables)-1]
}
