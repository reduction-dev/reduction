package snapshots

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"

	"reduction.dev/reduction/proto/snapshotpb"
	"reduction.dev/reduction/storage/locations"

	"google.golang.org/protobuf/proto"
)

var ErrCheckpointInProgress = errors.New("checkpoint in progress")

type Store struct {
	fileStore                  locations.StorageLocation
	savepointsPath             string
	checkpointsPath            string
	log                        *slog.Logger
	savepointURI               string
	subscriber                 chan CheckpointEvent
	retainedCheckpointsUpdated chan []uint64
	state                      storeState
	stateMu                    sync.Mutex
}

type CheckpointEvent struct {
	URI string
	Err error
}

type storeState struct {
	completedSnapshots []*jobSnapshot
	pendingSnapshot    *jobSnapshot
	checkpointID       uint64 // The last used, monotonically increasing checkpoint ID
}

type NewStoreParams struct {
	SavepointURI               string
	FileStore                  locations.StorageLocation
	SavepointsPath             string
	CheckpointsPath            string
	CheckpointEvents           chan CheckpointEvent
	RetainedCheckpointsUpdated chan []uint64
}

func NewStore(params *NewStoreParams) *Store {
	return &Store{
		fileStore:                  params.FileStore,
		savepointsPath:             params.SavepointsPath,
		checkpointsPath:            params.CheckpointsPath,
		log:                        slog.With("instanceID", "job"),
		savepointURI:               params.SavepointURI,
		subscriber:                 params.CheckpointEvents,
		retainedCheckpointsUpdated: params.RetainedCheckpointsUpdated,
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
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	if s.state.pendingSnapshot != nil {
		return 0, ErrCheckpointInProgress
	}
	s.state.checkpointID++
	s.state.pendingSnapshot = newJobSnapshot(s.state.checkpointID, operatorIDs, sourceRunnerIDs)

	return s.state.checkpointID, nil
}

func (s *Store) CreateSavepoint(operatorIDs, sourceRunnerIDs []string) (cpID uint64, created bool, err error) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	// If there is an in-progress checkpoint, mark it as a savepoint
	if s.state.pendingSnapshot != nil {
		if s.state.pendingSnapshot.isSavepoint {
			return 0, false, fmt.Errorf("savepoint already in-progress")
		}
		s.state.pendingSnapshot.isSavepoint = true
		return s.state.pendingSnapshot.id, false, nil
	}

	// Otherwise start a new checkpoint, marking it as a savepoint
	s.state.checkpointID++
	s.state.pendingSnapshot = newJobSnapshot(s.state.checkpointID, operatorIDs, sourceRunnerIDs)
	s.state.pendingSnapshot.isSavepoint = true

	return s.state.checkpointID, true, nil
}

func (s *Store) AddOperatorSnapshot(req *snapshotpb.OperatorCheckpoint) error {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	if s.state.pendingSnapshot == nil {
		return fmt.Errorf("operator %s tried to add to job checkpoint %d but there is no pending checkpoint", req.OperatorId, req.CheckpointId)
	}
	if s.state.pendingSnapshot.id != req.CheckpointId {
		return fmt.Errorf("operator %s tried to add to job checkpoint %d but pending checkpoint is %d", req.OperatorId, req.CheckpointId, s.state.pendingSnapshot.id)
	}

	if err := s.state.pendingSnapshot.addOperatorSnapshot(req); err != nil {
		s.log.Warn("adding operator snapshot", "err", err)
	}

	if s.state.pendingSnapshot.isComplete() {
		s.finishSnapshotAsync(s.state.pendingSnapshot)
		s.state.pendingSnapshot = nil
	}
	return nil
}

func (s *Store) AddSourceSnapshot(ckpt *snapshotpb.SourceCheckpoint) error {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	if s.state.pendingSnapshot == nil {
		return fmt.Errorf("source runner %s tried to add to job checkpoint %d but there is no pending snapshot", ckpt.SourceRunnerId, ckpt.CheckpointId)
	}
	if s.state.pendingSnapshot.id != ckpt.CheckpointId {
		return fmt.Errorf("source runner %s tried to add to job checkpoint %d but pending snapshot is %d", ckpt.SourceRunnerId, ckpt.CheckpointId, s.state.pendingSnapshot.id)
	}

	err := s.state.pendingSnapshot.addSourceSnapshot(ckpt)
	if err != nil {
		return err
	}

	if s.state.pendingSnapshot.isComplete() {
		s.finishSnapshotAsync(s.state.pendingSnapshot)
		s.state.pendingSnapshot = nil
	}
	return nil
}

func (s *Store) finishSnapshotAsync(snap *jobSnapshot) {
	go func() {
		uri, err := s.finishSnapshot(snap)
		s.subscriber <- CheckpointEvent{URI: uri, Err: err}
	}()
}

func (s *Store) finishSnapshot(snap *jobSnapshot) (uri string, err error) {
	data, err := snap.marshal()
	if err != nil {
		return "", err
	}

	uri, err = s.fileStore.Write(
		filepath.Join(s.checkpointsPath, "job-"+pathSegment(snap.id)+".snapshot"),
		bytes.NewBuffer(data))
	if err != nil {
		return "", fmt.Errorf("failed creating job snapshot: %v", err)
	}

	// Accessing state to update completedSnapshots
	s.stateMu.Lock()

	// When a new checkpoint is finished, all previous checkpoints are obsolete.
	if len(s.state.completedSnapshots) > 0 {
		obsoleteIDs := make([]uint64, 0, len(s.state.completedSnapshots))
		for _, oldSnap := range s.state.completedSnapshots {
			obsoleteIDs = append(obsoleteIDs, oldSnap.id)
		}

		// Delete the obsolete checkpoints files
		go func() {
			paths := make([]string, 0, len(obsoleteIDs))
			for _, id := range obsoleteIDs {
				paths = append(paths, filepath.Join(s.checkpointsPath, "job-"+pathSegment(id)+".snapshot"))
			}
			if err := s.fileStore.Remove(paths...); err != nil {
				s.log.Error("failed to remove obsolete checkpoint files", "paths", paths, "err", err)
			}
		}()

		// Notify subscribers of new list of checkpoints to retain (just the completed one)
		if s.retainedCheckpointsUpdated != nil {
			go func() {
				s.retainedCheckpointsUpdated <- []uint64{snap.id}
			}()
		}
	}

	// Reset the completed snapshots to remove obsolete checkpoints
	s.state.completedSnapshots = []*jobSnapshot{snap}
	s.stateMu.Unlock()

	s.log.Info("store wrote checkpoint", "uri", uri)

	if snap.isSavepoint {
		spURI, err := CreateSavepointArtifact(s.fileStore, s.savepointsPath, uri, snap)
		if err != nil {
			return "", err
		}
		s.log.Info("store wrote savepoint", "uri", spURI)
	}
	return uri, nil
}

func (s *Store) LoadLatestCheckpoint() (*snapshotpb.JobCheckpoint, error) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	// For a running job, check memory for checkpoints
	if len(s.state.completedSnapshots) > 0 {
		return s.state.completedSnapshots[len(s.state.completedSnapshots)-1].toProto(), nil
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
			s.state.checkpointID = snap.Id
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
	s.state.checkpointID = snap.Id

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
