package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"reduction.dev/reduction/connectors"
)

// SinkConfig contains configuration for the Kafka sink
type SinkConfig struct {
	Brokers []string
	Topic   string
}

// SinkWriter writes events to a Kafka topic.
type SinkWriter struct {
	client *kgo.Client
	topic  string
}

// NewSink creates a new Kafka SinkWriter.
func NewSink(config SinkConfig) (*SinkWriter, error) {
	client, err := kgo.NewClient(kgo.SeedBrokers(config.Brokers...))
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka client: %w", err)
	}

	return &SinkWriter{
		client: client,
		topic:  config.Topic,
	}, nil
}

// Write writes a single event to the Kafka topic.
func (s *SinkWriter) Write(event []byte) error {
	record := &kgo.Record{
		Topic: s.topic,
		Value: event,
	}

	ctx := context.Background()
	err := s.client.ProduceSync(ctx, record).FirstErr()
	if err != nil {
		return fmt.Errorf("kafka SinkWriter.Write failed: %w", err)
	}
	return nil
}

var _ connectors.SinkWriter = (*SinkWriter)(nil)
