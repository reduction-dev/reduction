package connectors

import (
	"context"
	"errors"

	"reduction.dev/reduction/proto/workerpb"
)

type SourceSplitter interface {
	IsSourceSplitter()

	// AssignSplits takes a list of IDs representing SourceReaders and returns a mapping
	// of SourceReader ID to a list of Splits.
	AssignSplits(ids []string) (map[string][]*workerpb.SourceSplit, error)

	// LoadCheckpoints gets a list of all checkpoint documents that were created
	// by SourceReaders Checkpoint() method calls.
	LoadCheckpoints([][]byte) error
}

type SplitAssignee interface {
	AssignSplits(ctx context.Context, sourceSplits []*workerpb.SourceSplit) error
}

type SourceSplit struct {
	SourceID string
	SplitID  string
}

type SourceReader interface {
	ReadEvents() ([][]byte, error)
	SetSplits(splits []*workerpb.SourceSplit) error
	Checkpoint() []byte
}

type SinkWriter interface {
	Write([]byte) error
}

type SourceConfig interface {
	Validate() error
	IsSourceConfig()
	NewSourceSplitter() SourceSplitter
	ProtoMessage() *workerpb.Source
}

type SinkConfig interface {
	Validate() error
	ProtoMessage() *workerpb.Sink
	IsSinkConfig()
}

var ErrEndOfInput = errors.New("end of input")
