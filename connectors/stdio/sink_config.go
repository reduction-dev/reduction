package stdio

import (
	"reduction.dev/reduction-protocol/jobconfigpb"
	"reduction.dev/reduction/connectors"
)

type SinkConfig struct{}

func (s *SinkConfig) IsSinkConfig() {}

func (s *SinkConfig) ProtoMessage() *jobconfigpb.Sink {
	return &jobconfigpb.Sink{
		Config: &jobconfigpb.Sink_Stdio{},
	}
}

func (s *SinkConfig) Validate() error {
	return nil
}

func SinkConfigFromProto(pb *jobconfigpb.StdioSink) *SinkConfig {
	return &SinkConfig{}
}

var _ connectors.SinkConfig = (*SinkConfig)(nil)
