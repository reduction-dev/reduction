package kafka_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	kafkapb "reduction.dev/reduction-protocol/kafkapb"
	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/connectors/kafka"
	"reduction.dev/reduction/proto/workerpb"
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
	var assignments map[string][]*workerpb.SourceSplit
	didAssign := make(chan struct{})
	splitter, err := kafka.NewSourceSplitter(config, []string{"r1"}, connectors.SourceSplitterHooks{
		AssignSplits: func(a map[string][]*workerpb.SourceSplit) {
			assignments = a
			close(didAssign)
		},
	}, nil)
	require.NoError(t, err)

	// Start the splitter and wait for it to assign splits
	splitter.Start()
	<-didAssign
	require.NoError(t, reader.AssignSplits(assignments["r1"]))

	var readEvents [][]byte
	assert.Eventually(t, func() bool {
		events, err := reader.ReadEvents()
		assert.NoError(t, err, "read events from kafka")
		readEvents = append(readEvents, events...)
		return len(readEvents) == len(eventsToWrite)
	}, 3*time.Second, 100*time.Millisecond, "should read all written events")

	// Unmarshal protobuf records and compare only the Value field
	var readValues [][]byte
	for _, ev := range readEvents {
		var pbRecord kafkapb.Record
		err := proto.Unmarshal(ev, &pbRecord)
		assert.NoError(t, err, "unmarshal kafkapb.Record")
		readValues = append(readValues, pbRecord.Value)
	}
	assert.ElementsMatch(t, eventsToWrite, readValues, "read events match written events")
}
