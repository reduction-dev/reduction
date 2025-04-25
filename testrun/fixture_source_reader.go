package testrun

import (
	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/proto/workerpb"
)

// fixtureSourceReader is a SourceReader that reads a slice of events.
type fixtureSourceReader struct {
	events [][]byte
}

func (f *fixtureSourceReader) Checkpoint() [][]byte {
	panic("unimplemented")
}

// ReadEvents reads all the events in one call and returns EOI.
func (f *fixtureSourceReader) ReadEvents() ([][]byte, error) {
	return f.events, connectors.ErrEndOfInput
}

func (f *fixtureSourceReader) AssignSplits(splits []*workerpb.SourceSplit) error {
	return nil
}

var _ connectors.SourceReader = &fixtureSourceReader{}
