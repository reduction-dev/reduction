package httpapi

import (
	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/proto/workerpb"
)

type SourceConfig struct {
	Addr   string
	Topics []string
}

func (s SourceConfig) Validate() error {
	return connectors.ValidateURL(s.Addr)
}

func (s SourceConfig) NewSourceSplitter() connectors.SourceSplitter {
	return NewSourceSplitter(s)
}

func (s SourceConfig) ProtoMessage() *workerpb.Source {
	return &workerpb.Source{
		Config: &workerpb.Source_HttpApiConfig{
			HttpApiConfig: &workerpb.Source_HTTPAPI{
				Addr:   s.Addr,
				Topics: s.Topics,
			},
		},
	}
}

func (s SourceConfig) IsSourceConfig() {}

var _ connectors.SourceConfig = SourceConfig{}

type SinkConfig struct {
	Addr string
}

func (s SinkConfig) Validate() error {
	return connectors.ValidateURL(s.Addr)
}

func (s SinkConfig) ProtoMessage() *workerpb.Sink {
	return &workerpb.Sink{
		Config: &workerpb.Sink_HttpApiConfig{
			HttpApiConfig: &workerpb.Sink_HTTPAPI{
				Addr: s.Addr,
			},
		},
	}
}

func (s SinkConfig) IsSinkConfig() {}

var _ connectors.SinkConfig = SinkConfig{}
