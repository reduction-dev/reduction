package sst

import (
	"iter"

	"reduction.dev/reduction/util/ds"
)

type Level struct {
	tables *ds.Set[*Table]
	Num    int
	// The total byte size of all tables in this level
	ByteSize int64
}

func (l Level) AllTables() iter.Seq[*Table] {
	return l.tables.All()
}

// Add adds new tables to a level and tracks the change in size
func (l Level) tablesAdded(tables ...*Table) Level {
	var addedSize int64
	for _, t := range tables {
		addedSize += t.Size()
	}

	return Level{
		tables:   l.tables.Added(tables...),
		Num:      l.Num,
		ByteSize: l.ByteSize + addedSize,
	}
}

// Add adds new tables to a level and tracks the change in size
func (l Level) tablesRemoved(tables ...*Table) Level {
	var removedSize int64
	for _, t := range tables {
		// Only count toward removed size if the table is in the set.
		if !l.tables.Has(t) {
			continue
		}

		removedSize += t.Size()
	}

	return Level{
		tables:   l.tables.Diff(ds.SetOf(tables...)),
		Num:      l.Num,
		ByteSize: l.ByteSize - removedSize,
	}
}

func (l Level) Clone() Level {
	return Level{
		tables:   l.tables,
		Num:      l.Num,
		ByteSize: l.ByteSize,
	}
}
