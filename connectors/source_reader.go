package connectors

import "reduction.dev/reduction/proto/workerpb"

type SourceReader interface {
	ReadEvents() ([][]byte, error)
	AssignSplits(splits []*workerpb.SourceSplit) error

	// Checkpoint returns a list of marshalled split states.
	Checkpoint() [][]byte
}

type UnimplementedSourceReader struct{}

func (u *UnimplementedSourceReader) Checkpoint() [][]byte {
	panic("unimplemented")
}

func (u *UnimplementedSourceReader) ReadEvents() ([][]byte, error) {
	panic("unimplemented")
}

func (u *UnimplementedSourceReader) AssignSplits(splits []*workerpb.SourceSplit) error {
	panic("unimplemented")
}

var _ SourceReader = (*UnimplementedSourceReader)(nil)
