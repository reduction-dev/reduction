package httpapi

import (
	"reduction.dev/reduction-protocol/jobconfigpb"
	"reduction.dev/reduction/connectors"
)

// SourceConfig contains configuration for the HTTP API source connector
type SourceConfig struct {
	Addr   string
	Topics []string
}

func (c SourceConfig) Validate() error {
	return connectors.ValidateURL(c.Addr)
}

func (c SourceConfig) NewSourceSplitter() connectors.SourceSplitter {
	return NewSourceSplitter(c)
}

func (c SourceConfig) ProtoMessage() *jobconfigpb.Source {
	return &jobconfigpb.Source{
		Config: &jobconfigpb.Source_HttpApi{
			HttpApi: &jobconfigpb.HTTPAPISource{
				Addr:   c.Addr,
				Topics: c.Topics,
			},
		},
	}
}

func (c SourceConfig) IsSourceConfig() {}

func SourceConfigFromProto(pb *jobconfigpb.HTTPAPISource) SourceConfig {
	return SourceConfig{
		Addr:   pb.Addr,
		Topics: pb.Topics,
	}
}

var _ connectors.SourceConfig = SourceConfig{}

type SinkConfig struct {
	Addr string
}

func (s SinkConfig) Validate() error {
	return connectors.ValidateURL(s.Addr)
}

func (s SinkConfig) ProtoMessage() *jobconfigpb.Sink {
	return &jobconfigpb.Sink{
		Config: &jobconfigpb.Sink_HttpApi{
			HttpApi: &jobconfigpb.HTTPAPISink{
				Addr: s.Addr,
			},
		},
	}
}

func (s SinkConfig) IsSinkConfig() {}

func SinkConfigFromProto(pb *jobconfigpb.HTTPAPISink) SinkConfig {
	return SinkConfig{
		Addr: pb.Addr,
	}
}

var _ connectors.SinkConfig = SinkConfig{}
