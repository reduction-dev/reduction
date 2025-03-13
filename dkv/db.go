package dkv

import (
	"fmt"
	"iter"
	"log/slog"
	"strings"
	"sync"

	"reduction.dev/reduction/dkv/bg"
	"reduction.dev/reduction/dkv/kv"
	"reduction.dev/reduction/dkv/memtable"
	"reduction.dev/reduction/dkv/recovery"
	"reduction.dev/reduction/dkv/sst"
	"reduction.dev/reduction/dkv/storage"
	"reduction.dev/reduction/dkv/wal"
	"reduction.dev/reduction/util/size"
)

var flushMemTablesQueue = bg.NewQueue(5)
var compactionQueue = bg.NewQueue(5)

type DB struct {
	fs          storage.FileSystem
	wal         *wal.Writer
	maxWALSize  uint64
	mtables     *memtable.List
	sstables    *sst.LevelList
	tableWriter *sst.TableWriter
	tasks       *bg.AsyncGroup
	compactor   *sst.Compactor
	checkpoints *recovery.CheckpointList
	seqNum      uint64 // The latest sequence number written
	mu          *sync.RWMutex
	logger      *slog.Logger
}

type Partition interface {
	OwnsKey(key []byte) bool
}

type DBOptions struct {
	FileSystem                  storage.FileSystem
	MemTableSize                uint64
	TargetFileSize              uint64
	MaxWALSize                  uint64 // Maximum size of the WAL before forcing a memtable flush
	NumLevels                   int
	L0TableNumCompactionTrigger int
	Partition                   Partition
	Logger                      *slog.Logger
}

func Open(options DBOptions, initCheckpoints []recovery.CheckpointHandle) *DB {
	db := New(options)

	if err := db.Start(initCheckpoints); err != nil {
		panic(fmt.Sprintf("db.boot: %v", err))
	}

	return db
}

func New(options DBOptions) *DB {
	// Default size to 64 MB
	if options.MemTableSize == 0 {
		options.MemTableSize = 64 * size.MB
	}
	// Default table file size to 256 MB
	if options.TargetFileSize == 0 {
		options.TargetFileSize = 256 * size.MB
	}
	// Default WAL size to 64 MB
	if options.MaxWALSize == 0 {
		options.MaxWALSize = 64 * size.MB
	}
	// Default L0 compaction trigger to 2
	if options.L0TableNumCompactionTrigger == 0 {
		options.L0TableNumCompactionTrigger = 2
	}
	// Default to 5 levels
	if options.NumLevels == 0 {
		options.NumLevels = 5
	}
	// Set a default logger
	if options.Logger == nil {
		options.Logger = slog.Default()
	}

	tw := sst.NewTableWriter(options.FileSystem, 0)
	compactor := &sst.Compactor{
		TableWriter:                 tw,
		L0RunNumCompactionTrigger:   options.L0TableNumCompactionTrigger,
		MaxSizeAmplificationPercent: 50,
		SmallestLevelSize:           256 * size.MB,
		LevelSizeMultiplier:         10,
		TargetTableSize:             int64(options.TargetFileSize),
	}

	db := &DB{
		mtables: memtable.NewList(&memtable.MemTableOptions{
			MemSize:   options.MemTableSize,
			NumLevels: options.NumLevels,
		}),
		sstables:    sst.NewEmptyLevelList(6),
		tableWriter: tw,
		maxWALSize:  options.MaxWALSize,
		compactor:   compactor,
		mu:          &sync.RWMutex{},
		fs:          options.FileSystem,
		tasks:       bg.NewAsyncGroup(),
		checkpoints: recovery.NewCheckpointList(),
		logger:      options.Logger,
	}

	return db
}

// Prepare the database. This loads the WAL entries into the memtable.
func (db *DB) Start(initCheckpoints []recovery.CheckpointHandle) error {
	checkpoints, err := recovery.LoadCheckpointList(db.fs, initCheckpoints)
	if err != nil {
		return fmt.Errorf("read checkpoint: %v", err)
	}
	db.checkpoints = checkpoints

	// Starting from scratch
	if db.checkpoints.IsEmpty() {
		db.wal = wal.NewWriter(db.fs, 0, db.maxWALSize)
		return nil
	}

	latestCP := db.checkpoints.Latest()
	db.sstables = latestCP.Levels
	db.seqNum = latestCP.Levels.LatestSeqNum

	// Start a new writer that doesn't write to a file yet.
	db.wal = wal.NewWriter(db.fs, latestCP.NextWALID(), db.maxWALSize)

	// Replay the WAL
	db.logger.Info("db.start replaying the WAL")
	for entry, err := range latestCP.WALSeq(db.fs) {
		if err != nil {
			return fmt.Errorf("reading wal: %v", err)
		}

		if entry.Deleted {
			db.Delete(entry.K)
		} else {
			db.Put(entry.K, entry.V)
		}
	}
	db.logger.Info("db.start done replaying the WAL")

	return nil
}

func (db *DB) Put(key []byte, value []byte) {
	nextSeqNum := db.seqNum + 1
	walFull := db.wal.Put(key, value, nextSeqNum)
	mtFull := db.mtables.Put(key, value, nextSeqNum)
	db.seqNum = nextSeqNum

	if walFull || mtFull {
		db.rotateMemtable()
	}
}

func (db *DB) Delete(key []byte) {
	nextSeqNum := db.seqNum + 1
	walFull := db.wal.Delete(key, nextSeqNum)
	mtFull := db.mtables.Delete(key, nextSeqNum)
	db.seqNum = nextSeqNum

	if walFull || mtFull {
		db.rotateMemtable()
	}
}

func (db *DB) Get(key []byte) (kv.Entry, error) {
	sstables := db.currentSSTables()

	// First try to get from the memtables
	v, err := db.mtables.Get(key)
	if err == nil {
		return v, nil
	}

	// Then try the SSTables
	if err == kv.ErrNotFound {
		return sstables.Get(key)
	}

	return nil, err
}

func (db *DB) ScanPrefix(prefix []byte, errOut *error) iter.Seq[kv.Entry] {
	sstables := db.currentSSTables()
	iters := []iter.Seq[kv.Entry]{db.mtables.ScanPrefix(prefix, errOut), sstables.ScanPrefix(prefix, errOut)}
	return kv.MergeEntries(iters)
}

// Checkpoint initiates a DB checkpoint associated with the caller's provided
// checkpoint ID. This ID must not be repeated between checkpoints.
func (db *DB) Checkpoint(ckptID uint64) (wait func() (recovery.CheckpointHandle, error)) {
	prevWAL := db.wal
	db.wal = db.wal.Rotate(db.fs)

	// Create a new checkpoint with the latest set of tables. Tables are
	// concurrently changing due to async flushing and compaction. They are also
	// incomplete because they don't include memtables.
	//
	// This method passes ownership of the tables ref to the new checkpoint
	db.checkpoints.Add(ckptID, db.currentSSTables(), prevWAL)

	return bg.Task2(func() (recovery.CheckpointHandle, error) {
		if err := prevWAL.Save(); err != nil {
			return recovery.CheckpointHandle{}, err
		}
		uri, err := db.checkpoints.Save(db.fs)
		if err != nil {
			return recovery.CheckpointHandle{}, err
		}
		return recovery.CheckpointHandle{
			CheckpointID: ckptID,
			URI:          uri,
		}, nil
	})
}

func (db *DB) RemoveCheckpoints(ids []uint64) error {
	db.checkpoints.Remove(ids)
	_, err := db.checkpoints.Save(db.fs)
	return err
}

func (db *DB) Close() error {
	return nil
}

func (db *DB) Diagnostics() string {
	var sb strings.Builder
	sb.WriteString(db.mtables.Diagnostics())
	sb.WriteString("\ndisk tables\n")

	sstables := db.currentSSTables()
	sb.WriteString(sstables.Diagnostics())

	return sb.String()
}

func (db *DB) WaitOnTasks() error {
	return db.tasks.Wait()
}

func (db *DB) rotateMemtable() {
	// Immediately replace the active table so that writes can continue.
	db.mtables.Rotate()
	// Segment the WAL so that obsolete entries can be dropped once the
	// memtables make it to the current sstables.
	db.wal.Cut()

	// Write sealed tables to sstables
	db.tasks.Enqueue(flushMemTablesQueue, func() error {
		sealedTables := db.mtables.Sealed()

		cs := &sst.ChangeSet{}
		for _, mt := range sealedTables {
			t, err := db.tableWriter.Write(mt.All())
			if err != nil {
				return err
			}
			cs.AddTables(0, t)
		}

		// Replace the set of sstables, clear old memtables, clear wal entries all
		// in one lock
		db.mu.Lock()
		db.sstables = db.sstables.NewWithChangeSet(cs)
		db.mtables.Dequeue(sealedTables)
		db.wal.Truncate(db.sstables.LatestSeqNum)
		db.mu.Unlock()

		// Run compact steps until there is no changeset
		db.tasks.Enqueue(compactionQueue, func() error {
			for {
				cs, err := db.compactor.Compact(db.currentSSTables())
				if err != nil {
					return err
				}
				if cs == nil {
					return nil
				}

				db.mu.Lock()
				db.sstables = db.sstables.NewWithChangeSet(cs)
				db.mu.Unlock()
			}
		})

		return nil
	})
}

func (db *DB) currentSSTables() *sst.LevelList {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.sstables
}
