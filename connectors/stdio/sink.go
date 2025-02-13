package stdio

import (
	"fmt"

	"reduction.dev/reduction/connectors"
)

type Sink struct{}

func NewSink(config SinkConfig) *Sink {
	return &Sink{}
}

func (s *Sink) Write(v []byte) error {
	fmt.Println(string(v))
	return nil
}

var _ connectors.SinkWriter = (*Sink)(nil)
