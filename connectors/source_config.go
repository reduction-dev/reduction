package connectors

import (
	"reduction.dev/reduction-protocol/jobconfigpb"
	"reduction.dev/reduction/proto/workerpb"
)

// Callbacks used for SourceSplitter to notify and get information from its call
// context.
type SourceSplitterHooks struct {
	AssignSplits func(assignments map[string][]*workerpb.SourceSplit)
}

// Callbacks used for SourceReader to notify and get information from its call
// context.
type SourceReaderHooks struct {
	NotifySplitsFinished func(splitIDs []string)
}

// Non-functional default hooks for SourceSplitter.
var NoOpSourceSplitterHooks = SourceSplitterHooks{
	AssignSplits: func(assignments map[string][]*workerpb.SourceSplit) {},
}

type SourceConfig interface {
	Validate() error
	NewSourceSplitter(sourceRunnerIDs []string, hooks SourceSplitterHooks, errChan chan<- error) SourceSplitter
	NewSourceReader(hooks SourceReaderHooks) SourceReader
	ProtoMessage() *jobconfigpb.Source
}
