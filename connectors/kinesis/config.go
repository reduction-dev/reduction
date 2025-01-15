package kinesis

import (
	"fmt"
	"net/url"

	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/proto/workerpb"
)

type SourceConfig struct {
	StreamARN string
	Endpoint  string
	Client    *Client
}

func (s SourceConfig) Validate() error {
	_, err := url.Parse(s.Endpoint)
	if err != nil {
		return fmt.Errorf("invalid kinesis Source endpoint: %s", s.Endpoint)
	}
	return nil
}

func (s SourceConfig) NewSourceSplitter() connectors.SourceSplitter {
	return NewSourceSplitter(s)
}

func (s SourceConfig) ProtoMessage() *workerpb.Source {
	return &workerpb.Source{
		Config: &workerpb.Source_KinesisConfig{
			KinesisConfig: &workerpb.Source_Kinesis{
				StreamArn: s.StreamARN,
				Endpoint:  s.Endpoint,
			},
		},
	}
}

func (s SourceConfig) IsSourceConfig() {}

var _ connectors.SourceConfig = SourceConfig{}
