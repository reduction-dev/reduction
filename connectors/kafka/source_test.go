package kafka_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"reduction.dev/reduction/connectors/kafka"
)

func TestKafkaSourceReader_ReadsAllEvents(t *testing.T) {
	ctx := t.Context()

	cluster := startKafka(t)
	defer cluster.client.Close()

	topic := "test-topic"
	cluster.CreateTopic(ctx, topic)

	numRecords := 10
	records := make([]*kgo.Record, 0, numRecords)
	for i := 1; i <= numRecords; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		records = append(records, &kgo.Record{Topic: topic, Value: []byte(value), Key: []byte(key)})
	}
	cluster.Produce(ctx, records...)

	config := kafka.SourceConfig{
		ConsumerGroup: "test-group",
		Brokers:       []string{"localhost:" + cluster.BrokerPort},
		Topics:        []string{topic},
	}

	reader1 := kafka.NewSourceReader(config)
	reader2 := kafka.NewSourceReader(config)

	splitter, err := kafka.NewSourceSplitter(config)
	require.NoError(t, err)
	assignments, err := splitter.AssignSplits([]string{"r1", "r2"})
	require.NoError(t, err)
	require.Len(t, assignments, 2)
	require.NotEmpty(t, assignments["r1"])
	require.NotEmpty(t, assignments["r2"])
	require.NoError(t, reader1.SetSplits(assignments["r1"]))
	require.NoError(t, reader2.SetSplits(assignments["r2"]))

	consumedEvents := make([]string, 0, numRecords)
	readAll := func(reader *kafka.SourceReader) {
		events, err := reader.ReadEvents()
		require.NoError(t, err, "read events")
		t.Log("read events:", events)
		for _, ev := range events {
			consumedEvents = append(consumedEvents, string(ev))
		}
	}
	assert.Eventually(t, func() bool {
		readAll(reader1)
		readAll(reader2)
		return len(consumedEvents) == numRecords
	}, 3*time.Second, 100*time.Millisecond, "all produced events should be consumed")
}
