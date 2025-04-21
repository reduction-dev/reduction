package embedded_test

import (
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/connectors/embedded"
	"reduction.dev/reduction/proto/workerpb"
	"reduction.dev/reduction/util/ds"
	"reduction.dev/reduction/util/iteru"
)

func TestGeneratingIncNums(t *testing.T) {
	config := embedded.SourceConfig{
		SplitCount:  2,
		BatchSize:   3,
		GeneratorID: "inc-nums",
	}
	sourceReaderIDs := []string{"sr1", "sr2"}
	var splitAssignments map[string][]*workerpb.SourceSplit

	didAssign := make(chan struct{})
	ss := embedded.NewSourceSplitter(config, sourceReaderIDs, connectors.SourceSplitterHooks{
		AssignSplits: func(assignments map[string][]*workerpb.SourceSplit) {
			splitAssignments = assignments
			close(didAssign)
		},
	})

	// Start the splitter and wait for it to assign splits
	ss.Start()
	<-didAssign

	// Create source readers and assign splits to them
	srs := ds.NewSortedMap[string, *embedded.SourceReader]()
	for id, splits := range splitAssignments {
		sr := embedded.NewSourceReader(config)
		sr.AssignSplits(splits)
		srs.Set(id, sr)
	}

	var events [][]byte
	for range iteru.Times(2) {
		for _, sr := range srs.Values() {
			readEvents, err := sr.ReadEvents()
			require.NoError(t, err)
			events = append(events, readEvents...)
		}
	}

	// 2 splits * 3 numbers per split read * 2 reads = 12
	assert.Len(t, events, 12)

	// Turn string-encoded []byte event numbers into []int.
	intEvents := iteru.Reduce(func(sum []int, numBytes []byte) []int {
		num, err := strconv.Atoi(string(numBytes))
		require.NoError(t, err)
		return append(sum, num)
	}, make([]int, 0, 40), slices.Values(events))
	assert.Equal(t, []int{
		0, 2, 4, // split 0
		1, 3, 5, // split 1
		6, 8, 10, // split 0
		7, 9, 11, // split 1
	}, intEvents)
}
