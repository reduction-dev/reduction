package httpapi

import (
	"encoding/json"

	"reduction.dev/reduction-protocol/jobconfigpb"
	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/proto/workerpb"
)

// ParseSourceConfig parses and validates an HTTP API source configuration
func ParseSourceConfig(data []byte) (SourceConfig, error) {
	var config SourceConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return SourceConfig{}, err
	}
	return config, nil
}

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

func (c SourceConfig) ProtoMessage() *workerpb.Source {
	return &workerpb.Source{
		Config: &workerpb.Source_HttpApiConfig{
			HttpApiConfig: &workerpb.Source_HTTPAPI{
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

func SinkConfigFromProto(pb *jobconfigpb.HTTPAPISink) SinkConfig {
	return SinkConfig{
		Addr: pb.Addr,
	}
}

var _ connectors.SinkConfig = SinkConfig{}
