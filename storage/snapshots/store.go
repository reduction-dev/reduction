package snapshots

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"

	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/proto/jobpb"
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
	subscriber                 chan string
	errChan                    chan error
	retainedCheckpointsUpdated chan []uint64
	state                      storeState
	stateMu                    sync.Mutex
	sourceSplitters            []connectors.SourceSplitter
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
	CheckpointEvents           chan string
	ErrChan                    chan error
	RetainedCheckpointsUpdated chan []uint64
}

func NewStore(params *NewStoreParams) *Store {
	store := &Store{
		fileStore:                  params.FileStore,
		savepointsPath:             params.SavepointsPath,
		checkpointsPath:            params.CheckpointsPath,
		log:                        slog.With("instanceID", "job"),
		savepointURI:               params.SavepointURI,
		subscriber:                 params.CheckpointEvents,
		retainedCheckpointsUpdated: params.RetainedCheckpointsUpdated,
	}

	return store
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
		s.finishSnapshot(s.state.pendingSnapshot)
		s.state.pendingSnapshot = nil
	}
	return nil
}

func (s *Store) AddSourceSnapshot(ckpt *jobpb.SourceRunnerCheckpointCompleteRequest) error {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	if s.state.pendingSnapshot == nil {
		return fmt.Errorf("source runner %s tried to add to job checkpoint %d but there is no pending snapshot", ckpt.SourceRunnerId, ckpt.CheckpointId)
	}
	if s.state.pendingSnapshot.id != ckpt.CheckpointId {
		return fmt.Errorf("source runner %s tried to add to job checkpoint %d but pending checkpoint is %d", ckpt.SourceRunnerId, ckpt.CheckpointId, s.state.pendingSnapshot.id)
	}

	err := s.state.pendingSnapshot.addSourceRunnerSnapshot(ckpt)
	if err != nil {
		return err
	}

	if s.state.pendingSnapshot.isComplete() {
		s.finishSnapshot(s.state.pendingSnapshot)
		s.state.pendingSnapshot = nil
	}
	return nil
}

func (s *Store) RegisterSourceSplitter(splitter connectors.SourceSplitter) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	s.sourceSplitters = append(s.sourceSplitters, splitter)
}

func (s *Store) finishSnapshot(snap *jobSnapshot) {
	// Get the source splitter checkpoints
	if len(s.sourceSplitters) != 1 {
		panic("only one source splitter is suppported")
	}
	snap.splitterState = s.sourceSplitters[0].Checkpoint()

	go func() {
		uri, err := s.finishSnapshotAsync(snap)
		if err != nil {
			s.errChan <- err
			return
		}
		if s.subscriber != nil {
			s.subscriber <- uri
		}
	}()
}

func (s *Store) finishSnapshotAsync(snap *jobSnapshot) (uri string, err error) {
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

// CurrentCheckpoint returns the latest checkpoint from memory.
func (s *Store) CurrentCheckpoint() *snapshotpb.JobCheckpoint {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	if len(s.state.completedSnapshots) > 0 {
		return s.state.completedSnapshots[len(s.state.completedSnapshots)-1].toProto()
	}
	return nil
}

// LoadCheckpoint loads the latest checkpoint from disk and stores it in
// memory.
func (s *Store) LoadCheckpoint() error {
	var loadedCheckpoint *snapshotpb.JobCheckpoint

	// For now let savepoint always override using local checkpoints. Later will
	// want to be able to start the job with an out-of-date savepoint to make
	// configuring a starting savepoint possible. This means that the checkpoints
	// will need to store their savepoint origin so that we know that a restart
	// should use the newer checkpoints and not go back to the origin savepoint.
	if s.savepointURI != "" {
		// Use the savepointURI when present
		snap, err := s.SnapshotForURI(s.savepointURI)
		if err != nil {
			return err
		}
		loadedCheckpoint = snap

		err = RestoreCheckpointFromSavepointArtifact(s.fileStore, s.savepointURI, snap)
		if err != nil {
			return fmt.Errorf("restore checkpoints from savepoint: %v", err)
		}
	} else {
		// For a new job, check the file store for first (latest) snapshot file.
		// Checkpoint IDs are encoded so that files will be in reverse chronological
		// order.
		var latestCheckpointFile string
		for filePath, err := range s.fileStore.List() {
			if err != nil {
				return err
			}
			if filepath.Ext(filePath) == ".snapshot" {
				latestCheckpointFile = filePath
				break
			}
		}

		if latestCheckpointFile == "" {
			return nil // No checkpoint to load
		}

		snap, err := s.SnapshotForURI(latestCheckpointFile)
		if err != nil {
			return err
		}
		loadedCheckpoint = snap
	}

	// Set the initial checkpoint ID counter
	s.state.checkpointID = loadedCheckpoint.Id

	// Create a job snapshot from the loaded checkpoint
	if len(loadedCheckpoint.SourceCheckpoints) != 1 {
		panic("only one source checkpoint is supported")
	}
	jSnapshot := &jobSnapshot{
		id:                  loadedCheckpoint.Id,
		operatorCheckpoints: loadedCheckpoint.OperatorCheckpoints,
		splitStates:         loadedCheckpoint.SourceCheckpoints[0].SplitStates,
		splitterState:       loadedCheckpoint.SourceCheckpoints[0].SplitterState,
	}
	s.state.completedSnapshots = []*jobSnapshot{jSnapshot}

	return nil
}
