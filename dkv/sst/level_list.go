package sst

import (
	"errors"
	"fmt"
	"iter"
	"math"
	"slices"
	"strings"

	"reduction.dev/reduction/dkv/kv"
	"reduction.dev/reduction/dkv/refs"
	"reduction.dev/reduction/dkv/storage"
	"reduction.dev/reduction/util/ds"
	"reduction.dev/reduction/util/sliceu"
)

type LevelList struct {
	levels       []Level
	refCount     *refs.RefCount
	LatestSeqNum uint64
}

const SizeAmplificationMax = math.MaxInt

func NewEmptyLevelList(levelCount int) *LevelList {
	if levelCount <= 0 {
		panic("disk tables must have at least one level")
	}
	ll := &LevelList{
		levels:       make([]Level, levelCount),
		refCount:     refs.NewRefCount(),
		LatestSeqNum: 0,
	}

	for i := range ll.levels {
		ll.levels[i] = Level{
			tables:   ds.NewSet[*Table](100),
			Num:      i,
			ByteSize: 0,
		}
	}

	return ll
}

func NewLevelListOfTables(tables [][]*Table) *LevelList {
	if len(tables) <= 0 {
		panic("disk tables must have at least one level")
	}

	var latestSeqNum uint64
	levels := make([]Level, len(tables))
	for i, levelTables := range tables {
		var byteSize int64
		levelTables := ds.NewSet[*Table](len(levelTables))
		for _, t := range tables[i] {
			latestSeqNum = max(latestSeqNum, t.endSeqNum)
			byteSize += t.Size()
			levelTables.Add(t)
		}
		levels[i] = Level{
			tables:   levelTables,
			Num:      i,
			ByteSize: byteSize,
		}
	}
	return &LevelList{
		refCount:     refs.NewRefCount(),
		levels:       levels,
		LatestSeqNum: latestSeqNum,
	}
}

func NewLevelListFromDocument(fs storage.FileSystem, llDoc [][]TableDocument) *LevelList {
	// Fill a matrix of levels using llDoc
	levels := make([][]*Table, len(llDoc))
	for i, levelDoc := range llDoc {
		levels[i] = make([]*Table, len(levelDoc))
		for j, tableDoc := range levelDoc {
			levels[i][j] = NewTableFromDocument(fs, tableDoc)
		}
	}

	return NewLevelListOfTables(levels)
}

func (ll *LevelList) Get(key []byte) (kv.Entry, error) {
	for t := range ll.AllTablesForKey(key) {
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

func (ll *LevelList) ScanPrefix(prefix []byte, errOut *error) iter.Seq[kv.Entry] {
	tables := slices.Collect(ll.AllTablesForPrefix(prefix))
	iters := make([]iter.Seq[kv.Entry], len(tables))
	for i, table := range tables {
		iters[i] = table.ScanPrefix(prefix, errOut)
	}

	// Return the merged entries without deleted records
	return func(yield func(kv.Entry) bool) {
		for entry := range kv.MergeEntries(iters) {
			// Skip deleted entries
			if entry.IsDelete() {
				continue
			}
			if !yield(entry) {
				return
			}
		}
	}
}

// A worst case estimate of the amount of extra space used. Zero means
// "perfect", 1 means 2x space used, 2 means 3x space used. The calculation is
// (size(R1) + size(R2) + ... size(Rn-1)) / size(Rn)
func (ll *LevelList) SizeAmplificationRatio() SAR {
	baseLevelSize := ll.At(-1).ByteSize

	// The compactionEligibleSize is the size of all the runs except the last.
	var compactionEligibleSize int64
	for _, level := range ll.levels[:len(ll.levels)-1] {
		compactionEligibleSize += int64(level.ByteSize)
	}

	return SAR{CompactionEligibleSize: compactionEligibleSize, BaseLevelSize: baseLevelSize}
}

func (ll *LevelList) TableCounts() []int {
	counts := make([]int, len(ll.levels))
	for i, l := range ll.levels {
		counts[i] = l.tables.Size()
	}
	return counts
}

func (ll *LevelList) AscendLevels(offset int) iter.Seq[Level] {
	if offset > len(ll.levels) {
		panic(fmt.Sprintf("can't offset %d levels, LevelList has %d levels", offset, len(ll.levels)))
	}
	return func(yield func(Level) bool) {
		for _, level := range slices.Backward(ll.levels[:len(ll.levels)-offset]) {
			if !yield(level) {
				return
			}
		}
	}
}

func (ll *LevelList) DescendLevels(offsets ...int) iter.Seq[Level] {
	if len(offsets) > 2 {
		panic(fmt.Sprintf("DescendLevels takes 0 to 2 offset arguments not %d", len(offsets)))
	}
	var start int
	var end int

	if len(offsets) == 0 {
		start = 0
		end = len(ll.levels)
	} else if len(offsets) == 1 {
		start = offsets[0]
		end = len(ll.levels)
	} else {
		start = offsets[0]
		end = offsets[1]
		// Allow negative end index like Python
		if end < 0 {
			end = len(ll.levels) + end
		}
	}

	if start > len(ll.levels) {
		panic(fmt.Sprintf("can't offset %d levels, LevelList has %d levels", start, len(ll.levels)))
	}
	return func(yield func(Level) bool) {
		for _, level := range ll.levels[start:end] {
			if !yield(level) {
				return
			}
		}
	}
}

func (ll *LevelList) AllTablesForKey(key []byte) iter.Seq[*Table] {
	return func(yield func(*Table) bool) {
		// go through L0 and collect any table that might have the key
		for t := range ll.At(0).AllTables() {
			if t.RangeContainsKey(key) {
				if !yield(t) {
					return
				}
			}
		}
		// go through each L1+ level and collect a table that might have the key
		for level := range ll.DescendLevels(1) {
			levelTables := level.tables.Slice()
			foundIndex, ok := sliceu.SearchUnique(levelTables, key, (*Table).RangeKeyCompare)
			if !ok {
				continue
			}
			if !yield(levelTables[foundIndex]) {
				return
			}
		}
	}
}

func (ll *LevelList) AllTablesForPrefix(prefix []byte) iter.Seq[*Table] {
	return func(yield func(*Table) bool) {
		// go through L0 and collect any table that could have the prefix
		for t := range ll.At(0).AllTables() {
			if t.RangeContainsPrefix(prefix) {
				if !yield(t) {
					return
				}
			}
		}
		// go through each L1+ level and collect a table that could have the prefix
		for level := range ll.DescendLevels(1) {
			levelTables := level.tables.Slice()
			foundIndex, ok := slices.BinarySearchFunc(levelTables, prefix, (*Table).RangePrefixCompare)
			if !ok {
				continue
			}
			for i := foundIndex; i < len(levelTables) && levelTables[i].RangeContainsPrefix(prefix); i++ {
				if !yield(levelTables[i]) {
					return
				}
			}
		}
	}
}

func (ll *LevelList) At(levelIndex int) Level {
	if levelIndex < 0 {
		levelIndex = len(ll.levels) + levelIndex
	}
	if levelIndex > len(ll.levels) {
		panic(fmt.Sprintf("tried to add to L%d which is out of range (there are %d levels)", levelIndex, len(ll.levels)))
	}
	return ll.levels[levelIndex]
}

func (ll *LevelList) AddTables(levelIndex int, tables ...*Table) {
	if levelIndex < 0 {
		levelIndex = len(ll.levels) + levelIndex
	}
	if levelIndex > len(ll.levels) {
		panic(fmt.Sprintf("tried to add to L%d which is out of range (there are %d levels)", levelIndex, len(ll.levels)))
	}
	ll.levels[levelIndex] = ll.levels[levelIndex].tablesAdded(tables...)
}

func (ll *LevelList) RemoveTables(tables []*Table) {
	// Track which tables are removed from the LevelList
	tablesRemoved := make(map[*Table]bool)

	for i := range ll.levels {
		for _, t := range tables {
			if ll.levels[i].tables.Has(t) {
				tablesRemoved[t] = true
			}
		}

		// Create new level with tables removed
		ll.levels[i] = ll.levels[i].tablesRemoved(tables...)
	}

	// Now drop references to tables that were present and removed
	for t := range tablesRemoved {
		t.DropRef()
	}
}

func (ll *LevelList) NewWithChangeSet(cs *ChangeSet) *LevelList {
	nextLevels := slices.Clone(ll.levels)

	// The new LevelList holds a new reference to tables in the source LevelList
	for _, l := range nextLevels {
		for t := range l.AllTables() {
			t.HoldRef()
		}
	}

	nextLL := &LevelList{
		refCount:     refs.NewRefCount(),
		levels:       nextLevels,
		LatestSeqNum: ll.LatestSeqNum,
	}

	for _, a := range cs.additions {
		nextLL.AddTables(a.LevelNum, a.Table)
		nextLL.LatestSeqNum = max(nextLL.LatestSeqNum, a.Table.endSeqNum)
	}
	nextLL.RemoveTables(cs.removals)

	return nextLL
}

func (ll *LevelList) Diagnostics() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("level count: %d", len(ll.levels)))

	for i, l := range ll.levels {
		sb.WriteString(fmt.Sprintf("\nlevel %d, tables %d, size %d", i, l.tables.Size(), l.ByteSize))
	}

	return sb.String()
}

// Hold an additional ref beyond the initial ref during instantiation.
func (ll *LevelList) HoldRef() {
	ll.refCount.Hold()
}

func (ll *LevelList) DropRef() error {
	var allErrs error
	released := ll.refCount.Drop()
	if released {
		for _, level := range ll.levels {
			for table := range level.tables.All() {
				err := table.DropRef()
				if err != nil {
					allErrs = errors.Join(err, allErrs)
				}
			}
		}
	}
	return allErrs
}

func (ll *LevelList) Document() [][]TableDocument {
	llDocs := make([][]TableDocument, len(ll.levels))
	for level := range ll.DescendLevels() {
		llDocs[level.Num] = make([]TableDocument, 0)
		for t := range level.AllTables() {
			llDocs[level.Num] = append(llDocs[level.Num], t.Document())
		}
	}

	return llDocs
}

// SAR provides calculations for the Space Amplification Ratio.
type SAR struct {
	CompactionEligibleSize int64
	BaseLevelSize          int64
}

func (r SAR) WithCompactedBytes(reduction int64) SAR {
	r.CompactionEligibleSize -= reduction
	return r
}

func (r SAR) Percentage() int {
	// If there is no data that could be compacted we'll call that perfect.
	if r.CompactionEligibleSize == 0 {
		return 0
	}
	// There is no data in the base level and there is some data that could be moved there
	// we'll use the highest int value to represent infinity.
	if r.BaseLevelSize == 0 {
		return math.MaxInt
	}
	return int(math.Round(float64(r.CompactionEligibleSize) / float64(r.BaseLevelSize) * 100))
}
