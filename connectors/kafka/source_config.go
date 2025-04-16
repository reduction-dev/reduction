package kafka

import (
	"fmt"

	"slices"

	"github.com/twmb/franz-go/pkg/kgo"
	"reduction.dev/reduction-protocol/jobconfigpb"
	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/proto/confvars"
)

// SourceConfig contains configuration for the Kafka source connector
type SourceConfig struct {
	ConsumerGroup string
	Brokers       []string
	Client        *kgo.Client
	Topics        []string
}

func (c SourceConfig) NewSourceReader() connectors.SourceReader {
	return NewSourceReader(c)
}

func (c SourceConfig) NewSourceSplitter() connectors.SourceSplitter {
	splitter, err := NewSourceSplitter(c)
	if err != nil {
		panic(fmt.Sprintf("failed to create Kafka source splitter: %v", err))
	}
	return splitter
}

func (c SourceConfig) ProtoMessage() *jobconfigpb.Source {
	return &jobconfigpb.Source{
		Config: &jobconfigpb.Source_Kafka{
			Kafka: &jobconfigpb.KafkaSource{
				ConsumerGroup: confvars.StringValue(c.ConsumerGroup),
				Brokers:       confvars.StringListValue(c.Brokers),
				Topics:        confvars.StringListValue(c.Topics),
			},
		},
	}
}

func (c SourceConfig) Validate() error {
	if c.ConsumerGroup == "" {
		return fmt.Errorf("KafkaSource consumer group is required")
	}
	if len(c.Brokers) == 0 {
		return fmt.Errorf("at least one broker is required for KafkaSource")
	}
	if slices.Contains(c.Brokers, "") {
		return fmt.Errorf("broker address cannot be empty")
	}
	if len(c.Topics) == 0 {
		return fmt.Errorf("at least one topic is required for KafkaSource")
	}
	return nil
}

var _ connectors.SourceConfig = SourceConfig{}
