package connectors

import "reduction.dev/reduction/proto/workerpb"

type SourceSplitter interface {
	IsSourceSplitter()

	// AssignSplits takes a list of IDs representing SourceReaders and returns a mapping
	// of SourceReader ID to a list of Splits.
	AssignSplits(ids []string) (map[string][]*workerpb.SourceSplit, error)

	// LoadCheckpoints gets a list of all checkpoint documents that were created
	// by SourceReaders Checkpoint() method calls.
	LoadCheckpoints([][]byte) error

	// Start allows the SourceSplitter to begin any background or async work.
	Start()

	// Close signals the SourceSplitter to stop and clean up resources.
	Close() error
}
