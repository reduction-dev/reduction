package httpapi

import (
	"net/http"

	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/proto/snapshotpb"
	"reduction.dev/reduction/proto/workerpb"
)

func NewSourceSplitter(config SourceConfig, sourceRunnerIDs []string, hooks connectors.SourceSplitterHooks, errChan chan<- error) *SourceSplitter {
	return &SourceSplitter{
		client:          &http.Client{},
		sourceRunnerIDs: sourceRunnerIDs,
		hooks:           hooks,
		errChan:         errChan,
	}
}

type SourceSplitter struct {
	client          *http.Client
	sourceRunnerIDs []string
	hooks           connectors.SourceSplitterHooks
	errChan         chan<- error
}

func (s *SourceSplitter) Start(ckpt *snapshotpb.SourceCheckpoint) error {
	var startingCursor []byte
	for _, d := range ckpt.GetSplitStates() {
		if len(d) != 0 {
			startingCursor = d
		}
	}

	assignments := make(map[string][]*workerpb.SourceSplit, len(s.sourceRunnerIDs))
	if len(s.sourceRunnerIDs) > 0 {
		assignments[s.sourceRunnerIDs[0]] = []*workerpb.SourceSplit{{
			SplitId:  "only",
			SourceId: "tbd",
			Cursor:   startingCursor,
		}}
	}
	s.hooks.AssignSplits(assignments)

	return nil
}

func (s *SourceSplitter) IsSourceSplitter() {}

func (s *SourceSplitter) Close() error { return nil }

func (s *SourceSplitter) NotifySplitsFinished(sourceRunnerID string, splitIDs []string) {}

func (s *SourceSplitter) Checkpoint() []byte {
	return nil
}

var _ connectors.SourceSplitter = (*SourceSplitter)(nil)
