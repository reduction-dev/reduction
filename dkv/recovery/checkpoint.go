package recovery

import (
	"iter"

	"reduction.dev/reduction/dkv/sst"
	"reduction.dev/reduction/dkv/storage"
	"reduction.dev/reduction/dkv/wal"
	"reduction.dev/reduction/util/iteru"
)

type Checkpoint struct {
	// The caller-provided checkpoint ID
	ID uint64
	// The WALs are "handles" to the WALs that are needed to recover the
	// data lost in memtables.
	WALs []wal.Handle
	// A snapshot of written SSTables
	Levels *sst.LevelList
}

func newCheckpointFromDocument(fs storage.FileSystem, doc checkpointDocument) *Checkpoint {
	walHandles := make([]wal.Handle, len(doc.WALs))
	for i, doc := range doc.WALs {
		walHandles[i] = wal.NewHandle(fs, doc)
	}

	return &Checkpoint{
		ID:     uint64(doc.ID),
		WALs:   walHandles,
		Levels: sst.NewLevelListFromDocument(fs, doc.Levels),
	}
}

func (cp *Checkpoint) Destroy() error {
	if err := cp.Levels.DropRef(); err != nil {
		return err
	}

	for _, wal := range cp.WALs {
		if err := wal.Delete(); err != nil {
			return err
		}
	}

	return nil
}

func (cp *Checkpoint) Document() checkpointDocument {
	if len(cp.WALs) > 1 {
		panic("should not serialize a checkpoint with multiple WALs")
	}
	doc := checkpointDocument{
		ID:     cp.ID,
		Levels: cp.Levels.Document(),
	}
	if len(cp.WALs) == 1 {
		doc.WALs = []wal.HandleDocument{cp.WALs[0].Document()}
	}

	return doc
}

// Return 1 greater than the largest WAL ID in this checkpoint.
func (cp *Checkpoint) NextWALID() int {
	var maxID int
	for _, h := range cp.WALs {
		maxID = max(maxID, h.ID)
	}
	return maxID + 1
}

// WALSeq provides an iterator that concatenates all the entries in the list of
// WALs.
func (cp *Checkpoint) WALSeq(fs storage.FileSystem) iter.Seq2[wal.Entry, error] {
	seqs := make([]iter.Seq2[wal.Entry, error], len(cp.WALs))
	for i, handle := range cp.WALs {
		seqs[i] = wal.NewReader(fs, handle).All()
	}
	return iteru.Concat2(seqs...)
}

// Example format:
//
//	{
//	  levels: [[Table, Table], [Table]],
//	  wal: 0,
//	}
type checkpointDocument struct {
	ID     uint64                `json:"id"`
	WALs   []wal.HandleDocument  `json:"wals"`   // The list of WAL files needed to recover memtable entries
	Levels [][]sst.TableDocument `json:"levels"` // The set of active SST files
	Refs   []string              `json:"refs"`
}
