package stdio

import (
	"reduction.dev/reduction-protocol/jobconfigpb"
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

func SinkConfigFromProto(pb *jobconfigpb.StdioSink) *SinkConfig {
	return &SinkConfig{}
}

var _ connectors.SinkConfig = (*SinkConfig)(nil)
