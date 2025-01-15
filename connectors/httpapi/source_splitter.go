package httpapi

import (
	"net/http"

	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/proto/workerpb"
)

func NewSourceSplitter(params SourceConfig) *SourceSplitter {
	return &SourceSplitter{&http.Client{}, nil}
}

type SourceSplitter struct {
	client         *http.Client
	startingCursor []byte
}

// Currently httpapi always has one split and assigns it to the first given ID.
func (s *SourceSplitter) AssignSplits(ids []string) (map[string][]*workerpb.SourceSplit, error) {
	assignments := make(map[string][]*workerpb.SourceSplit, len(ids))
	if len(ids) == 0 {
		return assignments, nil
	}

	assignments[ids[0]] = []*workerpb.SourceSplit{{
		SplitId:  "only",
		SourceId: "tbd",
		Cursor:   s.startingCursor,
	}}

	return assignments, nil
}

// LoadCheckpoints loads the one-and-only cursor for this SourceSplitter.
func (s *SourceSplitter) LoadCheckpoints(data [][]byte) error {
	for _, d := range data {
		if len(d) != 0 {
			s.startingCursor = d
		}
	}
	return nil
}

func (s *SourceSplitter) IsSourceSplitter() {}

var _ connectors.SourceSplitter = (*SourceSplitter)(nil)
