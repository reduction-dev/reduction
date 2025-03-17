package recovery

import (
	"encoding/json"
	"fmt"
	"io"
	"slices"

	"reduction.dev/reduction/dkv/sst"
	"reduction.dev/reduction/dkv/storage"
	"reduction.dev/reduction/dkv/wal"
	"reduction.dev/reduction/util/ds"
	"reduction.dev/reduction/util/sliceu"
)

const checkpointsFileName = "checkpoints"

type CheckpointList struct {
	checkpoints               []*Checkpoint
	checkpointsPendingRemoval []*Checkpoint // Track removed checkpoints so they can be destroyed on Save
}

func NewCheckpointList() *CheckpointList {
	return &CheckpointList{}
}

// CheckpointHandle is a reference to a checkpoints file.
type CheckpointHandle struct {
	CheckpointID uint64 // The caller provided checkpoint ID
	URI          string // A URI for the checkpoints file
}

// Add creates a new checkpoint, taking ownership of the LevelList reference.
// Checkpoint is responsible for the LevelList and WAL cleanup when destroyed.
func (cl *CheckpointList) Add(ckptID uint64, ll *sst.LevelList, w *wal.Writer) {
	cp := &Checkpoint{
		ID:     ckptID,
		Levels: ll,
		WALs:   []wal.Handle{w.Handle(ll.LatestSeqNum)},
	}
	cl.checkpoints = append(cl.checkpoints, cp)
}

func (cl *CheckpointList) Save(fs storage.FileSystem) (string, error) {
	// Collect a list of checkpoint docs for serialization
	checkpointDocs := make([]checkpointDocument, len(cl.checkpoints))
	for i, ckpt := range cl.checkpoints {
		checkpointDocs[i] = ckpt.Document()
	}
	doc := checkpointListDocument{
		Checkpoints: checkpointDocs,
	}

	// Serialize the checkpoint list document
	data, err := json.Marshal(doc)
	if err != nil {
		return "", err
	}

	// Save the data to a file
	file := fs.New(checkpointsFileName)
	if _, err = file.Write(data); err != nil {
		return "", err
	}
	if err := file.Save(); err != nil {
		return "", err
	}

	// Call cp.Destroy() to delete WAL files
	for _, cp := range cl.checkpointsPendingRemoval {
		if err := cp.Destroy(); err != nil {
			return "", err
		}
	}

	// Clear the list of pending checkpoints
	cl.checkpointsPendingRemoval = nil

	return file.URI(), nil
}

func (cl *CheckpointList) IsEmpty() bool {
	return len(cl.checkpoints) == 0
}

func (cl *CheckpointList) Latest() *Checkpoint {
	return cl.checkpoints[len(cl.checkpoints)-1]
}

// RetainOnly keeps only the checkpoints with the specified IDs in the list. Other checkpoints
// aren't really removed until the next successful Save.
func (cl *CheckpointList) RetainOnly(ids []uint64) {
	idsSet := ds.SetOf(ids...)
	nextCheckpoints := make([]*Checkpoint, 0, len(ids))
	for _, cp := range cl.checkpoints {
		if idsSet.Has(cp.ID) {
			nextCheckpoints = append(nextCheckpoints, cp)
		} else {
			cl.checkpointsPendingRemoval = append(cl.checkpointsPendingRemoval, cp)
		}
	}
	if len(nextCheckpoints) == 0 {
		panic(fmt.Sprintf("db missing the job's retained checkpoints; job_retained=%v, db_current=%v", ids, cl.checkpoints))
	}

	cl.checkpoints = nextCheckpoints
}

func (cl *CheckpointList) IncludesTable(uri string) bool {
	for _, cp := range cl.checkpoints {
		if cp.IncludesTable(uri) {
			return true
		}
	}
	return false
}

// LoadCheckpointList reads checkpoint files given by checkpointHandles into
// memory to create the CheckpointList. These checkpointHandles should point to
// files all belonging to the same aggregate checkpoint across DKV instances.
func LoadCheckpointList(fs storage.FileSystem, checkpointHandles []CheckpointHandle) (*CheckpointList, error) {
	if len(checkpointHandles) == 0 {
		return &CheckpointList{}, nil
	}

	ckptIDs := sliceu.UniqueFunc(checkpointHandles, func(h CheckpointHandle) uint64 {
		return h.CheckpointID
	})
	if len(ckptIDs) != 1 {
		panic(fmt.Sprintf("all initial checkpoints must have same ids but found %v", ckptIDs))
	}

	// short term: just pull out the one checkpoint ID. Longer term: need all checkpoints in the list before this checkpoint ID
	checkpointDocs := make([]checkpointDocument, len(checkpointHandles))
	for i, handle := range checkpointHandles {
		// Read all file data
		data, err := io.ReadAll(&storage.Cursor{File: fs.Open(handle.URI)})
		if err != nil {
			return nil, fmt.Errorf("reading checkpoint data from file %s: %v", handle.URI, err)
		}

		// Parse the checkpoint list document
		doc := checkpointListDocument{}
		if err := json.Unmarshal(data, &doc); err != nil {
			return nil, fmt.Errorf("parsing checkpoint data: %v", err)
		}

		// Only use the checkpoint indicated by the handle checkpoint ID for now.
		ckptIndex := slices.IndexFunc(doc.Checkpoints, func(doc checkpointDocument) bool {
			return uint64(doc.ID) == handle.CheckpointID
		})
		if ckptIndex == -1 {
			panic(fmt.Sprintf("failed to find indicated checkpoint ID %d in the checkpoints file %s", handle.CheckpointID, handle.URI))
		}

		checkpointDocs[i] = doc.Checkpoints[ckptIndex]
	}

	// Merge the checkpoint documents into the first
	compositeCheckpointDoc, rest := checkpointDocs[0], checkpointDocs[1:]
	for _, doc := range rest {
		// Merge WAL handles
		compositeCheckpointDoc.WALs = append(compositeCheckpointDoc.WALs, doc.WALs...)

		// Merge level list
		for levelIndex, level := range doc.Levels {
			compositeCheckpointDoc.Levels[levelIndex] = append(compositeCheckpointDoc.Levels[levelIndex], level...)
		}
	}

	compositeCheckpoint := newCheckpointFromDocument(fs, compositeCheckpointDoc)

	return &CheckpointList{checkpoints: []*Checkpoint{compositeCheckpoint}}, nil
}

// Example format:
//
//	{
//	  checkpoints: [{
//		  levels: [[Table, Table], [Table]],
//			wal: 0,
//	 	}, {
//		   ...
//		}]
//	}
type checkpointListDocument struct {
	Checkpoints []checkpointDocument `json:"checkpoints"`
}
