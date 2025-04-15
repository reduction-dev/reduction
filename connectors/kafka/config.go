package kafka

import (
	"github.com/twmb/franz-go/pkg/kgo"
)

// SourceConfig contains configuration for the Kafka source connector
type SourceConfig struct {
	ConsumerGroup string
	Brokers       []string
	Client        *kgo.Client
	Topics        []string
}

func (c SourceConfig) Validate() error {
	return nil
}
