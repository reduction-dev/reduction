package kafka_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"reduction.dev/reduction/connectors/kafka"
)

func TestSourceConfigValidate_ValidConfig(t *testing.T) {
	config := kafka.SourceConfig{
		ConsumerGroup: "group1",
		Brokers:       []string{"broker1:9092", "broker2:9092"},
		Topics:        []string{"topic1", "topic2"},
	}
	assert.NoError(t, config.Validate(), "expected valid config to pass validation")
}

func TestSourceConfigValidate_MissingConsumerGroup(t *testing.T) {
	config := kafka.SourceConfig{
		ConsumerGroup: "",
		Brokers:       []string{"broker1:9092"},
		Topics:        []string{"topic1"},
	}
	assert.ErrorContains(t, config.Validate(), "consumer group", "error should mention consumer group")
}

func TestSourceConfigValidate_EmptyBrokers(t *testing.T) {
	config := kafka.SourceConfig{
		ConsumerGroup: "group1",
		Brokers:       []string{},
		Topics:        []string{"topic1"},
	}
	assert.ErrorContains(t, config.Validate(), "broker", "error should mention broker")
}

func TestSourceConfigValidate_EmptyBrokerString(t *testing.T) {
	config := kafka.SourceConfig{
		ConsumerGroup: "group1",
		Brokers:       []string{"broker1:9092", ""},
		Topics:        []string{"topic1"},
	}
	assert.ErrorContains(t, config.Validate(), "broker address", "error should mention broker address")
}

func TestSourceConfigValidate_EmptyTopics(t *testing.T) {
	config := kafka.SourceConfig{
		ConsumerGroup: "group1",
		Brokers:       []string{"broker1:9092"},
		Topics:        []string{},
	}
	assert.ErrorContains(t, config.Validate(), "topic", "error should mention topic")
}
