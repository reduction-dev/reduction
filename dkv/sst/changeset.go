package sst

type ChangeSet struct {
	additions []TableAddition
	removals  []*Table
}

type TableAddition struct {
	LevelNum int
	Table    *Table
}

func (cs *ChangeSet) IsEmpty() bool {
	return len(cs.additions) == 0 && len(cs.removals) == 0
}

func (cs *ChangeSet) AddTables(level int, tables ...*Table) {
	for _, t := range tables {
		cs.additions = append(cs.additions, TableAddition{level, t})
	}
}

func (cs *ChangeSet) RemoveTables(tables ...*Table) {
	cs.removals = append(cs.removals, tables...)
}
