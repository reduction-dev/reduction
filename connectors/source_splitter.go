package connectors

type SourceSplitter interface {
	IsSourceSplitter()

	// LoadCheckpoints gets a list of all checkpoint documents that were created
	// by SourceReaders Checkpoint() method calls.
	LoadCheckpoints([][]byte) error

	// Start allows the SourceSplitter to begin any background or async work.
	Start()

	// Close signals the SourceSplitter to stop and clean up resources.
	Close() error
}
