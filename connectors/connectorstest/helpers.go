package connectorstest

import (
	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/proto/workerpb"
)

// AssignmentsFromSplitter is a test helper that starts a SourceSplitter with an AssignSplits hook
// that captures the assignments and signals when done. It returns the assignments and the splitter.
func AssignmentsFromSplitter(
	sourceConfig connectors.SourceConfig,
	sourceReaderIDs []string,
) map[string][]*workerpb.SourceSplit {
	didAssign := make(chan struct{})
	var splitAssignments map[string][]*workerpb.SourceSplit
	ss := sourceConfig.NewSourceSplitter(sourceReaderIDs, connectors.SourceSplitterHooks{
		AssignSplits: func(assignments map[string][]*workerpb.SourceSplit) {
			splitAssignments = assignments
			close(didAssign)
		},
	}, nil)
	ss.Start(nil)
	<-didAssign
	return splitAssignments
}

func StartSplitterAssignments(sourceConfig connectors.SourceConfig, sourceReaderIDs []string) (connectors.SourceSplitter, chan map[string][]*workerpb.SourceSplit) {
	assignmentsChannel := make(chan map[string][]*workerpb.SourceSplit, 1)
	splitter := sourceConfig.NewSourceSplitter(sourceReaderIDs, connectors.SourceSplitterHooks{
		AssignSplits: func(assignments map[string][]*workerpb.SourceSplit) {
			assignmentsChannel <- assignments
		},
	}, nil)
	splitter.Start(nil)

	return splitter, assignmentsChannel
}
