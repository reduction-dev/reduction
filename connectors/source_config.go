package connectors

import (
	"reduction.dev/reduction-protocol/jobconfigpb"
	"reduction.dev/reduction/proto/workerpb"
)

// Callbacks used for SourceSplitter to notify and get information from its call
// context.
type SourceSplitterHooks struct {
	OnSplitAssignmentsUpdated func(assignments map[string][]*workerpb.SourceSplit)
}

// Non-functional default hooks for SourceSplitter.
var NoOpSourceSplitterHooks = SourceSplitterHooks{
	OnSplitAssignmentsUpdated: func(assignments map[string][]*workerpb.SourceSplit) {},
}

type SourceConfig interface {
	Validate() error
	NewSourceSplitter(hooks SourceSplitterHooks) SourceSplitter
	NewSourceReader() SourceReader
	ProtoMessage() *jobconfigpb.Source
}
