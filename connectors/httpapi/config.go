package httpapi

import (
	"reduction.dev/reduction-protocol/jobconfigpb"
	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/proto/confvars"
)

// SourceConfig contains configuration for the HTTP API source connector
type SourceConfig struct {
	Addr   string
	Topics []string
}

func (c SourceConfig) Validate() error {
	return connectors.ValidateURL(c.Addr)
}

func (c SourceConfig) NewSourceSplitter(hooks connectors.SourceSplitterHooks) connectors.SourceSplitter {
	return NewSourceSplitter(c, hooks)
}

func (c SourceConfig) NewSourceReader() connectors.SourceReader {
	return NewSourceReader(c)
}

func (c SourceConfig) ProtoMessage() *jobconfigpb.Source {
	return &jobconfigpb.Source{
		Config: &jobconfigpb.Source_HttpApi{
			HttpApi: &jobconfigpb.HTTPAPISource{
				Addr:   confvars.StringValue(c.Addr),
				Topics: c.Topics,
			},
		},
	}
}

func SourceConfigFromProto(pb *jobconfigpb.HTTPAPISource) SourceConfig {
	return SourceConfig{
		Addr:   pb.Addr.GetValue(),
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
				Addr: confvars.StringValue(s.Addr),
			},
		},
	}
}

func (s SinkConfig) NewSink() connectors.SinkWriter {
	return NewSink(s)
}

func SinkConfigFromProto(pb *jobconfigpb.HTTPAPISink) SinkConfig {
	return SinkConfig{
		Addr: pb.Addr.GetValue(),
	}
}

var _ connectors.SinkConfig = SinkConfig{}
