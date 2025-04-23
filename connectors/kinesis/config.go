package kinesis

import (
	"fmt"
	"net/url"
	"strings"

	"reduction.dev/reduction-protocol/jobconfigpb"
	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/proto/confvars"
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

	if c.StreamARN == "" {
		return fmt.Errorf("kinesis Source stream ARN is required")
	}

	if !strings.HasPrefix(c.StreamARN, "arn:aws:kinesis:") {
		return fmt.Errorf("invalid StreamARN for kinesis source: %s", c.StreamARN)
	}

	return nil
}

func (c SourceConfig) NewSourceSplitter(sourceRunnerIDs []string, hooks connectors.SourceSplitterHooks, errChan chan<- error) connectors.SourceSplitter {
	return NewSourceSplitter(c, sourceRunnerIDs, hooks, errChan)
}

func (c SourceConfig) NewSourceReader(hooks connectors.SourceReaderHooks) connectors.SourceReader {
	return NewSourceReader(c, hooks)
}

func (c SourceConfig) ProtoMessage() *jobconfigpb.Source {
	return &jobconfigpb.Source{
		Config: &jobconfigpb.Source_Kinesis{
			Kinesis: &jobconfigpb.KinesisSource{
				StreamArn: confvars.StringValue(c.StreamARN),
				Endpoint:  confvars.StringValue(c.Endpoint),
			},
		},
	}
}

func SourceConfigFromProto(pb *jobconfigpb.KinesisSource) SourceConfig {
	return SourceConfig{
		StreamARN: pb.StreamArn.GetValue(),
		Endpoint:  pb.Endpoint.GetValue(),
	}
}

var _ connectors.SourceConfig = SourceConfig{}
