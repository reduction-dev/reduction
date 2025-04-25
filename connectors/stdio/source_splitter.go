package stdio

import (
	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/proto/snapshotpb"
	"reduction.dev/reduction/proto/workerpb"
)

type SourceSplitter struct{}

func (s *SourceSplitter) IsSourceSplitter() {}

func (s *SourceSplitter) SetSplits(splits []*workerpb.SourceSplit) error {
	// No split assignment needed for stdin
	return nil
}

func (s *SourceSplitter) Checkpoint() []byte {
	return nil // No checkpointing needed for stdin
}

func (s *SourceSplitter) LoadCheckpoint(ckpt *snapshotpb.SourceCheckpoint) error {
	// No checkpointing needed for stdin
	return nil
}

func (s *SourceSplitter) Start() {}

func (s *SourceSplitter) Close() error { return nil }

func (s *SourceSplitter) NotifySplitsFinished(sourceRunnerID string, splitIDs []string) {}

var _ connectors.SourceSplitter = (*SourceSplitter)(nil)
