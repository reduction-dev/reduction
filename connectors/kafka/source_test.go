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

func TestKafkaSourceReader_Checkpoint(t *testing.T) {
	ctx := t.Context()

	cluster := startKafka(t)
	defer cluster.client.Close()

	topic := "test-topic"
	cluster.CreateTopic(ctx, topic)

	numRecords := 20
	records := make([]*kgo.Record, 0, numRecords)
	for i := 1; i <= numRecords; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		records = append(records, &kgo.Record{Topic: topic, Value: []byte(value), Key: []byte(key)})
	}

	// Write first 10 records
	cluster.Produce(ctx, records[:10]...)

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
		for _, ev := range events {
			consumedEvents = append(consumedEvents, string(ev))
		}
	}
	// Read first 10 records
	assert.Eventually(t, func() bool {
		readAll(reader1)
		readAll(reader2)
		return len(consumedEvents) == 10
	}, 3*time.Second, 100*time.Millisecond, "should consume first 10 records")

	// Checkpoint
	cp1 := reader1.Checkpoint()
	cp2 := reader2.Checkpoint()

	// Write remaining 10 records
	cluster.Produce(ctx, records[10:]...)

	// Create new readers
	reader3 := kafka.NewSourceReader(config)
	reader4 := kafka.NewSourceReader(config)

	splitter2, err := kafka.NewSourceSplitter(config)
	require.NoError(t, err)
	err = splitter2.LoadCheckpoints([][]byte{cp1, cp2})
	require.NoError(t, err)
	assignments2, err := splitter2.AssignSplits([]string{"r3", "r4"})
	require.NoError(t, err)
	require.Len(t, assignments2, 2)
	require.NoError(t, reader3.SetSplits(assignments2["r3"]))
	require.NoError(t, reader4.SetSplits(assignments2["r4"]))

	// Read remaining 10 records
	assert.EventuallyWithT(t, func(t *assert.CollectT) {
		readAll(reader3)
		readAll(reader4)
		assert.Len(t, consumedEvents, 20)
	}, 3*time.Second, 100*time.Millisecond, "should consume all 20 records after checkpoint")

	// Assert all records read (order not guaranteed across partitions)
	expected := make([]string, 0, numRecords)
	for i := 1; i <= numRecords; i++ {
		expected = append(expected, fmt.Sprintf("value-%d", i))
	}
	assert.ElementsMatch(t, expected, consumedEvents, "all records read after checkpoint")
}
