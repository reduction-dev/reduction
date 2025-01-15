package sst

import (
	"errors"
	"fmt"
	"iter"
	"slices"

	"reduction.dev/reduction/dkv/kv"
	"reduction.dev/reduction/util/iteru"
)

type Compactor struct {
	L0RunNumCompactionTrigger   int
	MaxSizeAmplificationPercent int
	SmallestLevelSize           int64
	LevelSizeMultiplier         int
	TableWriter                 *TableWriter
	TargetTableSize             int64

	// Track the current level that needs evaluation for compaction between
	// Compact calls.
	minorCompactionLevel int
}

var ErrNoCompactionNeeded = errors.New("no compaction needed")

// Perform a compaction step on the given LevelList. When no compaction
// step is needed, Compact return a nil ChangeSet and no error.
func (c *Compactor) Compact(levels *LevelList) (*ChangeSet, error) {
	// If there are aren't enough L0 tables, don't start compaction
	if c.minorCompactionLevel == 0 && levels.At(0).tables.Size() < c.L0RunNumCompactionTrigger {
		return nil, nil
	}

	sar := levels.SizeAmplificationRatio()
	if sar.Percentage() > c.MaxSizeAmplificationPercent {
		cs, err := c.majorCompaction(levels, sar)
		if err != nil {
			return nil, err
		}
		return cs, nil
	}

	return c.minorCompaction(levels)
}

// Look at LMax - 1 and try to pull in files to satisfy MaxSizeAmplificationPercent.
// The heuristic is to subtract enough size from lower tables to satisfy the
// requirement assuming (worst case) that all writes will be compacted away in
// lower levels.
func (c *Compactor) majorCompaction(levels *LevelList, sar SAR) (*ChangeSet, error) {
	// Go through all non-base levels from oldest to newest and pick tables to
	// merge into base level.
	var tablesToMerge []*Table
	for level := range levels.AscendLevels(1) {
		tableIter := slices.SortedFunc(level.AllTables(), OrderOldToNew)

		// Keep adding tables and recalculating SAR until we've met the Space
		// Amplification goal.
		for _, candidate := range tableIter {
			sar = sar.WithCompactedBytes(int64(candidate.Size()))
			tablesToMerge = append(tablesToMerge, candidate)
			if sar.Percentage() < c.MaxSizeAmplificationPercent {
				break
			}
		}
	}

	// Include base tables in the merge
	for t := range levels.At(-1).AllTables() {
		tablesToMerge = append(tablesToMerge, t)
	}

	// Then merge newer tables and base layer together
	var scanErr error
	tableIters := make([]iter.Seq[kv.Entry], len(tablesToMerge))
	for i, t := range tablesToMerge {
		tableIters[i] = t.ScanPrefix(nil, &scanErr)
	}
	entries := kv.MergeEntries(tableIters)
	newTables, err := c.TableWriter.WriteRun(entries, uint64(c.TargetTableSize))
	if err != nil {
		return nil, fmt.Errorf("major compaction writing table %v", err)
	}
	if scanErr != nil {
		return nil, fmt.Errorf("major compaction scanning table %v", scanErr)
	}

	cs := &ChangeSet{}
	cs.AddTables(-1, newTables...)
	cs.RemoveTables(tablesToMerge...)
	return cs, nil

	// Later: figure out key range to query in base level, for now read all.
}

func (c *Compactor) minorCompaction(levels *LevelList) (*ChangeSet, error) {
	// If we're just starting the minor compaction merge L0 tables into L1.
	if c.minorCompactionLevel == 0 {
		c.minorCompactionLevel++

		// Take all files from L0 and merge them into L1
		inputTables := slices.Collect(iteru.Concat(levels.At(0).AllTables(), levels.At(1).AllTables()))

		var scanErr error
		tableIters := make([]iter.Seq[kv.Entry], len(inputTables))
		for i, t := range inputTables {
			tableIters[i] = t.ScanPrefix(nil, &scanErr)
		}

		entries := kv.MergeEntries(tableIters)
		newL1Tables, err := c.TableWriter.WriteRun(entries, uint64(c.TargetTableSize))
		if scanErr != nil {
			return nil, scanErr
		}
		if err != nil {
			return nil, err
		}

		// Create new levels with the merged input tables removed and the newly
		// created table from the merge added.
		cs := &ChangeSet{}
		cs.AddTables(1, newL1Tables...)
		cs.RemoveTables(inputTables...)
		return cs, nil
	}

	// Try compacting remaining levels before the last
	for c.minorCompactionLevel < len(levels.levels)-1 {
		level := levels.At(c.minorCompactionLevel)
		c.minorCompactionLevel++

		// If the level size is now over the limit, merge into the next level.
		if level.ByteSize > c.SmallestLevelSize*int64(level.Num) {
			// Assume we merge all tables in these levels for now. Later will need sorted runs.
			mergeTables := slices.Collect(iteru.Concat(level.AllTables(), levels.At(level.Num+1).AllTables()))

			var scanErr error
			tableIters := make([]iter.Seq[kv.Entry], len(mergeTables))
			for i, t := range mergeTables {
				tableIters[i] = t.ScanPrefix(nil, &scanErr)
			}

			entries := kv.MergeEntries(tableIters)
			newTable, err := c.TableWriter.WriteRun(entries, uint64(c.TargetTableSize))
			if scanErr != nil {
				return nil, scanErr
			}
			if err != nil {
				return nil, err
			}

			cs := &ChangeSet{}
			cs.AddTables(level.Num+1, newTable...)
			cs.RemoveTables(mergeTables...)
			return cs, nil
		}
	}

	c.minorCompactionLevel = 0
	return nil, nil
}
