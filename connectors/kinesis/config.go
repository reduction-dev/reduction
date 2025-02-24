package kinesis

import (
	"fmt"
	"net/url"

	"reduction.dev/reduction-protocol/jobconfigpb"
	"reduction.dev/reduction/connectors"
)

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

func (c SourceConfig) NewSourceReader() connectors.SourceReader {
	return NewSourceReader(c)
}

func (c SourceConfig) ProtoMessage() *jobconfigpb.Source {
	return &jobconfigpb.Source{
		Config: &jobconfigpb.Source_Kinesis{
			Kinesis: &jobconfigpb.KinesisSource{
				StreamArn: c.StreamARN,
				Endpoint:  c.Endpoint,
			},
		},
	}
}

func SourceConfigFromProto(pb *jobconfigpb.KinesisSource) SourceConfig {
	return SourceConfig{
		StreamARN: pb.StreamArn,
		Endpoint:  pb.Endpoint,
	}
}

var _ connectors.SourceConfig = SourceConfig{}
