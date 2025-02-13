package stdio

import (
	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/proto/workerpb"
)

type SinkConfig struct{}

func (s *SinkConfig) IsSinkConfig() {}

func (s *SinkConfig) ProtoMessage() *workerpb.Sink {
	return &workerpb.Sink{
		Config: &workerpb.Sink_StdioConfig{},
	}
}

func (s *SinkConfig) Validate() error {
	return nil
}

var _ connectors.SinkConfig = (*SinkConfig)(nil)
