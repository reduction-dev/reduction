package snapshots

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"

	"reduction.dev/reduction/proto/snapshotpb"
	"reduction.dev/reduction/storage"

	"google.golang.org/protobuf/proto"
)

var ErrCheckpointInProgress = errors.New("checkpoint in progress")

type Store struct {
	fileStore          storage.FileStore
	savepointsPath     string
	checkpointsPath    string
	completedSnapshots []*jobSnapshot
	pendingSnapshot    *jobSnapshot
	log                *slog.Logger
	savepointURI       string
	checkpointID       uint64 // The last used, monotonically increasing checkpoint ID
}

type NewStoreParams struct {
	SavepointURI    string
	FileStore       storage.FileStore
	SavepointsPath  string
	CheckpointsPath string
}

func NewStore(params *NewStoreParams) *Store {
	return &Store{
		fileStore:       params.FileStore,
		savepointsPath:  params.SavepointsPath,
		checkpointsPath: params.CheckpointsPath,
		log:             slog.With("instanceID", "job"),
		savepointURI:    params.SavepointURI,
	}
}

func (s *Store) SnapshotForURI(uri string) (*snapshotpb.JobCheckpoint, error) {
	data, err := s.fileStore.Read(uri)
	if err != nil {
		return nil, err
	}

	var snap snapshotpb.JobCheckpoint
	err = proto.Unmarshal(data, &snap)

	return &snap, err
}

func (s *Store) SavepointURIForID(id uint64) (string, error) {
	uri, err := s.fileStore.URI(filepath.Join(s.savepointsPath, pathSegment(id), "job.savepoint"))
	if err != nil {
		return "", fmt.Errorf("failed checking for savepoint existence (%d): %w", id, err)
	}
	return uri, nil
}

func (s *Store) CreateCheckpoint(operatorIDs, sourceRunnerIDs []string) (uint64, error) {
	if s.pendingSnapshot != nil {
		return 0, ErrCheckpointInProgress
	}
	s.checkpointID++
	s.pendingSnapshot = newJobSnapshot(s.checkpointID, operatorIDs, sourceRunnerIDs)

	return s.checkpointID, nil
}

func (s *Store) CreateSavepoint(operatorIDs, sourceRunnerIDs []string) (cpID uint64, created bool, err error) {
	// If there is an in-progress checkpoint, mark it as a savepoint
	if s.pendingSnapshot != nil {
		if s.pendingSnapshot.isSavepoint {
			return 0, false, fmt.Errorf("savepoint already in-progress")
		}
		s.pendingSnapshot.isSavepoint = true
		return s.pendingSnapshot.id, false, nil
	}

	// Otherwise start a new checkpoint, marking it as a savepoint
	s.checkpointID++
	s.pendingSnapshot = newJobSnapshot(s.checkpointID, operatorIDs, sourceRunnerIDs)
	s.pendingSnapshot.isSavepoint = true

	return s.checkpointID, true, nil
}

func (s *Store) AddOperatorSnapshot(req *snapshotpb.OperatorCheckpoint) error {
	if s.pendingSnapshot == nil {
		return fmt.Errorf("operator %s tried to add to job checkpoint %d but there is no pending checkpoint", req.OperatorId, req.CheckpointId)
	}
	if s.pendingSnapshot.id != req.CheckpointId {
		return fmt.Errorf("operator %s tried to add to job checkpoint %d but pending checkpoint is %d", req.OperatorId, req.CheckpointId, s.pendingSnapshot.id)
	}

	if err := s.pendingSnapshot.addOperatorSnapshot(req); err != nil {
		s.log.Warn("adding operator snapshot", "err", err)
	}

	if s.pendingSnapshot.isComplete() {
		return s.finishSnapshot()
	}
	return nil
}

func (s *Store) AddSourceSnapshot(ckpt *snapshotpb.SourceCheckpoint) error {
	if s.pendingSnapshot == nil {
		return fmt.Errorf("source runner %s tried to add to job checkpoint %d but there is no pending snapshot", ckpt.SourceRunnerId, ckpt.CheckpointId)
	}
	if s.pendingSnapshot.id != ckpt.CheckpointId {
		return fmt.Errorf("source runner %s tried to add to job checkpoint %d but pending snapshot is %d", ckpt.SourceRunnerId, ckpt.CheckpointId, s.pendingSnapshot.id)
	}

	err := s.pendingSnapshot.addSourceSnapshot(ckpt)
	if err != nil {
		return err
	}

	if s.pendingSnapshot.isComplete() {
		return s.finishSnapshot()
	}
	return nil
}

func (s *Store) finishSnapshot() error {
	finalizingSnapshot := s.pendingSnapshot
	data, err := finalizingSnapshot.marshal()
	if err != nil {
		return err
	}

	uri, err := s.fileStore.Write(
		filepath.Join(s.checkpointsPath, "job-"+pathSegment(finalizingSnapshot.id)+".snapshot"),
		bytes.NewBuffer(data))
	if err != nil {
		return fmt.Errorf("failed creating job snapshot: %v", err)
	}
	s.completedSnapshots = append(s.completedSnapshots, finalizingSnapshot)

	s.log.Info("store wrote checkpoint", "uri", uri)
	s.pendingSnapshot = nil

	if finalizingSnapshot.isSavepoint {
		spURI, err := CreateSavepointArtifact(s.fileStore, s.savepointsPath, uri, finalizingSnapshot)
		if err != nil {
			return err
		}
		s.log.Info("store wrote savepoint", "uri", spURI)
	}

	return nil
}

func (s *Store) LoadLatestCheckpoint() (*snapshotpb.JobCheckpoint, error) {
	// For a running job, check memory for checkpoints
	if len(s.completedSnapshots) > 0 {
		return s.completedSnapshots[len(s.completedSnapshots)-1].toProto(), nil
	}

	// For now let savepoint always override using local checkpoints. Later will
	// want to be able to start the job with an out-of-date checkpoint to make
	// configuring a starting savepoint possible. This means that the checkpoints
	// will need to store their savepoint origin so that we know that a restart
	// should use the newer checkpoints and not go back to the origin savepoint.
	if s.savepointURI == "" {
		// For a new job, check the file store for first (latest) snapshot file.
		// Checkpoint IDs are encoded so that files will be in reverse chronological
		// order.
		var latestSnapshotFile string
		for filePath, err := range s.fileStore.List() {
			if err != nil {
				return nil, err
			}
			if filepath.Ext(filePath) == ".snapshot" {
				latestSnapshotFile = filePath
				break
			}
		}
		if latestSnapshotFile != "" {
			snap, err := s.SnapshotForURI(latestSnapshotFile)
			s.checkpointID = snap.Id
			return snap, err
		}

		// No checkpoints and no savepoint to start from
		return nil, nil
	}

	// We're starting a job with no checkpoint history so use the savepoint if available
	snap, err := s.SnapshotForURI(s.savepointURI)
	if err != nil {
		return nil, err
	}
	s.checkpointID = snap.Id

	if err := RestoreFromSavepointArtifact(s.fileStore, s.savepointURI, snap); err != nil {
		return nil, fmt.Errorf("restore checkpoints from savepoint: %v", err)
	}

	return snap, nil
}

func (s *Store) LatestCheckpointURI() (string, error) {
	var lastPath string
	for filePath, err := range s.fileStore.List() {
		lastPath = filePath
		if err != nil {
			return "", err
		}
	}

	return lastPath, nil
}
