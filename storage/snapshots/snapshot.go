package snapshots

import (
	"fmt"
	"log/slog"
	"maps"
	"slices"

	"reduction.dev/reduction/proto/snapshotpb"
	"reduction.dev/reduction/util/iteru"

	"google.golang.org/protobuf/proto"
)

type jobSnapshot struct {
	// The snapshot ID
	id uint64

	// A set of operator IDs that we're expecting a response from
	operatorIDsComplete map[string]bool

	// Acknowledged operator snapshots
	operatorCheckpoints []*snapshotpb.OperatorCheckpoint

	// A set of source runner IDs that we're expecting a response from
	sourceRunnerIDsComplete map[string]bool

	// Acknowledged source runner snapshots
	sourceRunnerSnapshots []*snapshotpb.SourceCheckpoint

	// A flag to indicate the checkpoint should be converted to a savepoint
	isSavepoint bool
}

func newJobSnapshot(checkpointID uint64, operatorIDs, sourceRunnerIDs []string) *jobSnapshot {
	wIDs := make(map[string]bool, len(operatorIDs))
	for _, id := range operatorIDs {
		wIDs[id] = false
	}

	srIDs := make(map[string]bool, len(sourceRunnerIDs))
	for _, id := range sourceRunnerIDs {
		srIDs[id] = false
	}

	return &jobSnapshot{
		id:                      checkpointID,
		operatorIDsComplete:     wIDs,
		sourceRunnerIDsComplete: srIDs,
	}
}

func (s *jobSnapshot) addOperatorSnapshot(req *snapshotpb.OperatorCheckpoint) error {
	wasCompleted, ok := s.operatorIDsComplete[req.OperatorId]
	if !ok {
		ids := slices.Collect(maps.Keys(s.operatorIDsComplete))
		return fmt.Errorf("received snapshot from unexpected operator %s, expected operators %v", req.OperatorId, ids)
	}
	if wasCompleted {
		return fmt.Errorf("received checkpoint (%d) from operator (%s) that already sent one", req.CheckpointId, req.OperatorId)
	}

	s.operatorIDsComplete[req.OperatorId] = true
	s.operatorCheckpoints = append(s.operatorCheckpoints, req)

	return nil
}

func (s *jobSnapshot) addSourceSnapshot(ckpt *snapshotpb.SourceCheckpoint) error {
	wasCompleted, ok := s.sourceRunnerIDsComplete[ckpt.SourceRunnerId]
	if !ok {
		ids := slices.Collect(maps.Keys(s.sourceRunnerIDsComplete))
		return fmt.Errorf("received source runner checkpoint with unknown id id=%s, expectedIDs=%v", ckpt.SourceRunnerId, ids)
	}
	if wasCompleted {
		slog.Warn("received another source runner checkpoint from same id", "id", ckpt.SourceRunnerId)
	}

	s.sourceRunnerIDsComplete[ckpt.SourceRunnerId] = true
	s.sourceRunnerSnapshots = append(s.sourceRunnerSnapshots, ckpt)

	return nil
}

func (s *jobSnapshot) isComplete() bool {
	return iteru.Every(maps.Values(s.sourceRunnerIDsComplete)) &&
		iteru.Every(maps.Values(s.operatorIDsComplete))
}

func (s *jobSnapshot) toProto() *snapshotpb.JobCheckpoint {
	return &snapshotpb.JobCheckpoint{
		Id:                  s.id,
		SourceCheckpoints:   s.sourceRunnerSnapshots,
		OperatorCheckpoints: s.operatorCheckpoints,
	}
}

func (s *jobSnapshot) marshal() ([]byte, error) {
	return proto.Marshal(s.toProto())
}
