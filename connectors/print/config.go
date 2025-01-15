package print

import (
	"fmt"

	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/proto/workerpb"
)

type PrintSink struct{}

func NewSink(config SinkConfig) *PrintSink {
	return &PrintSink{}
}

func (s *PrintSink) Write(v []byte) error {
	fmt.Println(string(v))
	return nil
}

var _ connectors.SinkWriter = (*PrintSink)(nil)

type SinkConfig struct{}

func (s *SinkConfig) IsSinkConfig() {}

func (s *SinkConfig) ProtoMessage() *workerpb.Sink {
	return &workerpb.Sink{
		Config: &workerpb.Sink_PrintConfig{},
	}
}

func (s *SinkConfig) Validate() error {
	return nil
}

var _ connectors.SinkConfig = (*SinkConfig)(nil)
