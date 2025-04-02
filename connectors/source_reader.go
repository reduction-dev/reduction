package connectors

import "reduction.dev/reduction/proto/workerpb"

type SourceReader interface {
	ReadEvents() ([][]byte, error)
	SetSplits(splits []*workerpb.SourceSplit) error
	Checkpoint() []byte
}

type UnimplementedSourceReader struct{}

func (u *UnimplementedSourceReader) Checkpoint() []byte {
	panic("unimplemented")
}

func (u *UnimplementedSourceReader) ReadEvents() ([][]byte, error) {
	panic("unimplemented")
}

func (u *UnimplementedSourceReader) SetSplits(splits []*workerpb.SourceSplit) error {
	panic("unimplemented")
}

var _ SourceReader = (*UnimplementedSourceReader)(nil)
