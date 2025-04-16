package kafka_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"reduction.dev/reduction/connectors/kafka"
)

func TestSinkWriter_WriteAndReadBack(t *testing.T) {
	integrationOnly(t)
	ctx := t.Context()
	cluster := startKafka(t)
	defer cluster.Close()

	topic := "test-sink-topic"
	cluster.CreateTopic(ctx, topic)

	sink, err := kafka.NewSink(kafka.SinkConfig{
		Brokers: []string{cluster.BrokerAddr},
		Topic:   topic,
	})
	require.NoError(t, err)

	eventsToWrite := [][]byte{
		[]byte("sink-d1"),
		[]byte("sink-d2"),
		[]byte("sink-d3"),
	}
	for _, e := range eventsToWrite {
		err := sink.Write(e)
		assert.NoError(t, err, "write event to kafka sink")
	}

	// Read back the events using a SourceReader
	config := kafka.SourceConfig{
		ConsumerGroup: "test-sink-group",
		Brokers:       []string{cluster.BrokerAddr},
		Topics:        []string{topic},
	}
	reader := kafka.NewSourceReader(config)
	splitter, err := kafka.NewSourceSplitter(config)
	require.NoError(t, err)
	assignments, err := splitter.AssignSplits([]string{"r1"})
	require.NoError(t, err)
	require.NoError(t, reader.SetSplits(assignments["r1"]))

	var readEvents [][]byte
	assert.Eventually(t, func() bool {
		events, err := reader.ReadEvents()
		assert.NoError(t, err, "read events from kafka")
		readEvents = append(readEvents, events...)
		return len(readEvents) == len(eventsToWrite)
	}, 3*time.Second, 100*time.Millisecond, "should read all written events")

	assert.ElementsMatch(t, eventsToWrite, readEvents, "read events match written events")
}
