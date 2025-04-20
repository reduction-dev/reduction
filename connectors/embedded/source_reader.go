package embedded

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strconv"

	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/proto/workerpb"
	"reduction.dev/reduction/util/iteru"
)

type SourceReader struct {
	splits    []*split
	generator *eventGenerator
}

type split struct {
	Cursor  int
	SplitID string
}

func NewSourceReader(config SourceConfig) *SourceReader {
	return &SourceReader{
		generator: &eventGenerator{
			splitCount: config.SplitCount,
			batchSize:  config.BatchSize,
		},
	}
}

type HTTPSourceEvent struct {
	Events [][]byte
	Status string
	Cursor int
}

// ReadEvents reads a batch of events from each assigned split.
func (s *SourceReader) ReadEvents() ([][]byte, error) {
	var events [][]byte
	for _, split := range s.splits {
		cursor, splitEvents := s.generator.ReadSplit(split)
		split.Cursor = cursor
		events = append(events, splitEvents...)
	}

	return events, nil
}

// TODO: Change the semantics of this method so that it only adds new splits
// and does not remove any existing splits.
func (s *SourceReader) AssignSplits(splits []*workerpb.SourceSplit) error {
	nextSplits := make([]*split, len(splits))
	for i, sp := range splits {
		readerSplit := &split{SplitID: sp.SplitId}

		// Read the binary cursor if available
		if len(sp.Cursor) != 0 {
			var cursor int64
			err := binary.Read(bytes.NewBuffer(sp.Cursor), binary.BigEndian, &cursor)
			if err != nil {
				return err
			}
			readerSplit.Cursor = int(cursor)
		}

		nextSplits[i] = readerSplit
	}

	s.splits = nextSplits
	return nil
}

// Checkpoint returns a JSON document representing an array of splits.
func (s *SourceReader) Checkpoint() []byte {
	data, err := json.Marshal(s.splits)
	if err != nil {
		panic(fmt.Sprintf("BUG marshaling splits: %v", err))
	}
	return data
}

var _ connectors.SourceReader = (*SourceReader)(nil)

type eventGenerator struct {
	splitCount int
	batchSize  int
}

// ReadSplit generates the next 10 consecutive numbers for a split and returns
// those as events.
func (g *eventGenerator) ReadSplit(split *split) (cursor int, events [][]byte) {
	// Get the numeric split index from the splitID string.
	splitIndex, err := strconv.Atoi(split.SplitID)
	if err != nil {
		panic(err)
	}

	// Collect the next batchSize numbers for this shard index.
	events = make([][]byte, 0, g.batchSize)
	for i := range iteru.Times(g.batchSize) {
		nextNum := (splitIndex + split.Cursor) + (g.splitCount * i)
		events = append(events, []byte(strconv.Itoa(nextNum)))
	}

	return split.Cursor + g.splitCount*g.batchSize, events
}
