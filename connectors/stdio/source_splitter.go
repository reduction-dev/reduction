package stdio

import (
	"reduction.dev/reduction/connectors"
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

func (s *SourceSplitter) LoadCheckpoints(data [][]byte) error {
	// No checkpointing needed for stdin
	return nil
}

func (s *SourceSplitter) AssignSplits(ids []string) (map[string][]*workerpb.SourceSplit, error) {
	return map[string][]*workerpb.SourceSplit{}, nil // No split assignment needed for stdin
}

var _ connectors.SourceSplitter = (*SourceSplitter)(nil)
