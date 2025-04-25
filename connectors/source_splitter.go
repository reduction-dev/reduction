package connectors

import "reduction.dev/reduction/proto/snapshotpb"

type SourceSplitter interface {
	IsSourceSplitter()

	// LoadCheckpoints gets a list of all checkpoint documents that were created
	// by SourceReaders Checkpoint() method calls.
	LoadCheckpoint(*snapshotpb.SourceCheckpoint) error

	// Start allows the SourceSplitter to begin any background or async work.
	Start()

	// Close signals the SourceSplitter to stop and clean up resources.
	Close() error

	// NotifySplitsFinished informs the splitter that the given splits have been finished.
	NotifySplitsFinished(sourceRunnerID string, splitIDs []string)

	// Checkpoint returns a snapshot of the splitter's state for checkpointing.
	Checkpoint() []byte
}

type UnimplementedSourceSplitter struct{}

// Checkpoint implements SourceSplitter.
func (u *UnimplementedSourceSplitter) Checkpoint() []byte {
	panic("unimplemented")
}

// Close implements SourceSplitter.
func (u *UnimplementedSourceSplitter) Close() error {
	panic("unimplemented")
}

// IsSourceSplitter implements SourceSplitter.
func (u *UnimplementedSourceSplitter) IsSourceSplitter() {
	panic("unimplemented")
}

// LoadCheckpoint implements SourceSplitter.
func (u *UnimplementedSourceSplitter) LoadCheckpoint(*snapshotpb.SourceCheckpoint) error {
	panic("unimplemented")
}

// NotifySplitsFinished implements SourceSplitter.
func (u *UnimplementedSourceSplitter) NotifySplitsFinished(sourceRunnerID string, splitIDs []string) {
	panic("unimplemented")
}

// Start implements SourceSplitter.
func (u *UnimplementedSourceSplitter) Start() {
	panic("unimplemented")
}

var _ SourceSplitter = (*UnimplementedSourceSplitter)(nil)
