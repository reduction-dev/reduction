package embedded

import (
	"strconv"

	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/proto/snapshotpb"
	"reduction.dev/reduction/proto/workerpb"
	"reduction.dev/reduction/util/iteru"
	"reduction.dev/reduction/util/sliceu"
)

func NewSourceSplitter(config SourceConfig, sourceRunnerIDs []string, hooks connectors.SourceSplitterHooks) *SourceSplitter {
	return &SourceSplitter{
		splitCount:      config.SplitCount,
		sourceRunnerIDs: sourceRunnerIDs,
		hooks:           hooks,
	}
}

type SourceSplitter struct {
	splitCount      int
	sourceRunnerIDs []string
	hooks           connectors.SourceSplitterHooks
}

func (s *SourceSplitter) Start(ckpt *snapshotpb.SourceCheckpoint) error {
	// Embedded SourceSplitter does not checkpoint
	if ckpt != nil {
		panic("embedded source splitter does not support checkpointing")
	}

	// Create splits all with nil cursors
	sourceSplits := make([]*workerpb.SourceSplit, s.splitCount)
	for splitIndex := range iteru.Times(s.splitCount) {
		splitID := strconv.Itoa(splitIndex)
		sourceSplits[splitIndex] = &workerpb.SourceSplit{
			SplitId:  splitID,
			SourceId: "TBD",
			Cursor:   nil,
		}
	}

	// Assign the splits to the source runner IDs
	assignments := make(map[string][]*workerpb.SourceSplit, len(s.sourceRunnerIDs))
	splitGroups := sliceu.Partition(sourceSplits, len(s.sourceRunnerIDs))
	for i, id := range s.sourceRunnerIDs {
		assignments[id] = splitGroups[i]
	}
	s.hooks.AssignSplits(assignments)

	return nil
}

func (s *SourceSplitter) IsSourceSplitter() {}

func (s *SourceSplitter) Close() error { return nil }

func (s *SourceSplitter) NotifySplitsFinished(sourceRunnerID string, splitIDs []string) {}

func (s *SourceSplitter) Checkpoint() []byte { return nil }

var _ connectors.SourceSplitter = (*SourceSplitter)(nil)
