package connectors

import "reduction.dev/reduction/proto/workerpb"

type SourceReader interface {
	ReadEvents() ([][]byte, error)
	SetSplits(splits []*workerpb.SourceSplit) error
	Checkpoint() []byte
}
