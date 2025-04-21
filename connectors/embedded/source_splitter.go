package embedded

import (
	"encoding/binary"
	"encoding/json"
	"strconv"

	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/proto/workerpb"
	"reduction.dev/reduction/util/iteru"
	"reduction.dev/reduction/util/sliceu"
)

func NewSourceSplitter(config SourceConfig, sourceRunnerIDs []string, hooks connectors.SourceSplitterHooks) *SourceSplitter {
	return &SourceSplitter{
		splitCount:         config.SplitCount,
		checkpointedSplits: make(map[string]*workerpb.SourceSplit),
		sourceRunnerIDs:    sourceRunnerIDs,
		hooks:              hooks,
	}
}

type SourceSplitter struct {
	splitCount         int
	checkpointedSplits map[string]*workerpb.SourceSplit
	sourceRunnerIDs    []string
	hooks              connectors.SourceSplitterHooks
}

func (s *SourceSplitter) Start() {
	// Pretend to "discover" splits here.
	sourceSplits := make([]*workerpb.SourceSplit, s.splitCount)
	for splitIndex := range iteru.Times(s.splitCount) {
		splitID := strconv.Itoa(splitIndex)
		sourceSplits[splitIndex] = &workerpb.SourceSplit{
			SplitId:  splitID,
			SourceId: "TBD",
			Cursor:   s.checkpointedSplits[splitID].GetCursor(), // Use checkpointed cursors if they exist
		}
	}

	// Assign the splits to the source runner IDs
	assignments := make(map[string][]*workerpb.SourceSplit, len(s.sourceRunnerIDs))
	splitGroups := sliceu.Partition(sourceSplits, len(s.sourceRunnerIDs))
	for i, id := range s.sourceRunnerIDs {
		assignments[id] = splitGroups[i]
	}
	s.hooks.AssignSplits(assignments)
}

// LoadCheckpoints gets a list of lists of split documents
func (s *SourceSplitter) LoadCheckpoints(docs [][]byte) error {
	// For each checkpoint document
	for _, doc := range docs {
		// Parse the JSON document
		var splits []*split
		if err := json.Unmarshal(doc, &splits); err != nil {
			return err
		}

		// Append to the list of protobuf objects to assign
		for _, split := range splits {
			cursorBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(cursorBytes, uint64(split.Cursor))

			s.checkpointedSplits[split.SplitID] = &workerpb.SourceSplit{
				SplitId:  split.SplitID,
				SourceId: "TBD",
				Cursor:   cursorBytes,
			}
		}
	}
	return nil
}

func (s *SourceSplitter) IsSourceSplitter() {}

func (s *SourceSplitter) Close() error { return nil }

func (s *SourceSplitter) NotifySplitsFinished(sourceRunnerID string, splitIDs []string) {}

var _ connectors.SourceSplitter = (*SourceSplitter)(nil)
