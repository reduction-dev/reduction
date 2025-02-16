package kinesis

import (
	"encoding/json"
	"fmt"
	"net/url"

	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/proto/workerpb"
)

// ParseSourceConfig parses and validates a Kinesis source configuration
func ParseSourceConfig(data []byte) (SourceConfig, error) {
	var config SourceConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return SourceConfig{}, err
	}
	return config, nil
}

// SourceConfig contains configuration for the Kinesis source connector
type SourceConfig struct {
	StreamARN string
	Endpoint  string
	Client    *Client
}

func (c SourceConfig) Validate() error {
	_, err := url.Parse(c.Endpoint)
	if err != nil {
		return fmt.Errorf("invalid kinesis Source endpoint: %s", c.Endpoint)
	}
	return nil
}

func (c SourceConfig) NewSourceSplitter() connectors.SourceSplitter {
	return NewSourceSplitter(c)
}

func (c SourceConfig) ProtoMessage() *workerpb.Source {
	return &workerpb.Source{
		Config: &workerpb.Source_KinesisConfig{
			KinesisConfig: &workerpb.Source_Kinesis{
				StreamArn: c.StreamARN,
				Endpoint:  c.Endpoint,
			},
		},
	}
}

func (c SourceConfig) IsSourceConfig() {}

var _ connectors.SourceConfig = SourceConfig{}
