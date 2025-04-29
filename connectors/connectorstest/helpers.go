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
