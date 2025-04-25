package kafka

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	rpkafkapb "reduction.dev/reduction-protocol/kafkapb"
	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/connectors/kafka/kafkapb"
	"reduction.dev/reduction/proto/workerpb"
)

type SourceReader struct {
	client  *kgo.Client
	offsets Offsets
}

func NewSourceReader(config SourceConfig) *SourceReader {
	if config.Client == nil {
		var err error
		config.Client, err = kgo.NewClient(kgo.SeedBrokers(config.Brokers...))
		if err != nil {
			panic(fmt.Sprintf("failed to create Kafka client: %v", err))
		}
	}
	return &SourceReader{
		client:  config.Client,
		offsets: NewOffsets(),
	}
}

func (s *SourceReader) AssignSplits(splits []*workerpb.SourceSplit) error {
	newOffsets := NewOffsets()
	for _, split := range splits {
		parts := strings.Split(split.SplitId, ":")
		if len(parts) != 2 {
			return fmt.Errorf("invalid split id: %s", split.SplitId)
		}
		partition, err := strconv.Atoi(parts[1])
		if err != nil {
			return fmt.Errorf("invalid partition in split id: %s", split.SplitId)
		}
		offset := int64(0)
		if len(split.Cursor) > 0 {
			offset, err = strconv.ParseInt(string(split.Cursor), 10, 64)
			if err != nil {
				return fmt.Errorf("invalid cursor for split %s: %w", split.SplitId, err)
			}
		}
		topic := parts[0]
		s.offsets.Set(topic, int32(partition), offset)
		newOffsets.Set(topic, int32(partition), offset)
	}

	s.client.AddConsumePartitions(newOffsets.AsPartitions())
	return nil
}

func (s *SourceReader) ReadEvents() ([][]byte, error) {
	if len(s.offsets) == 0 {
		return [][]byte{}, nil
	}

	ctx := context.Background()
	fetches := s.client.PollFetches(ctx)
	if fetches.Err() != nil {
		return nil, fmt.Errorf("kafka SourceReader.ReadEvents: %w", connectors.NewTerminalError(fetches.Err()))
	}

	var events [][]byte
	iter := fetches.RecordIter()
	for !iter.Done() {
		record := iter.Next()
		// Set the offset to the next offset to read
		s.offsets.Set(record.Topic, record.Partition, record.Offset+1)

		pbRecord := &rpkafkapb.Record{
			Topic:     record.Topic,
			Partition: record.Partition,
			Key:       record.Key,
			Value:     record.Value,
			Timestamp: timestamppb.New(record.Timestamp),
		}
		if len(record.Headers) > 0 {
			headers := make([]*rpkafkapb.Header, 0, len(record.Headers))
			for _, h := range record.Headers {
				headers = append(headers, &rpkafkapb.Header{
					Key:   h.Key,
					Value: h.Value,
				})
			}
			pbRecord.Headers = headers
		}
		data, err := proto.Marshal(pbRecord)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal kafkapb.Record: %w", err)
		}
		events = append(events, data)
	}
	return events, nil
}

// Checkpoint turns the offsets map into a flat list of marshalled split states.
func (s *SourceReader) Checkpoint() [][]byte {
	var splitStates [][]byte
	for topicName, partMap := range s.offsets {
		for partition, offset := range partMap {
			splitState := &kafkapb.SplitState{
				Topic:    topicName,
				Parition: partition,
				Offset:   offset,
			}
			data, err := proto.Marshal(splitState)
			if err != nil {
				panic(fmt.Sprintf("failed to marshal split state: %v", err))
			}
			splitStates = append(splitStates, data)
		}
	}
	return splitStates
}

var _ connectors.SourceReader = (*SourceReader)(nil)

type Offsets map[string]map[int32]int64

func NewOffsets() Offsets {
	return make(Offsets)
}

// Set sets the offset for a topic/partition.
func (o Offsets) Set(topic string, partition int32, offset int64) {
	if o[topic] == nil {
		o[topic] = make(map[int32]int64)
	}
	o[topic][partition] = offset
}

// Get returns the offset and true if present.
func (o Offsets) Get(topic string, partition int32) (int64, bool) {
	partitionMap, ok := o[topic]
	if !ok {
		return 0, false
	}
	offset, ok := partitionMap[partition]
	return offset, ok
}

// AsPartitions converts the offset to the type expected by client.AddConsumePartitions.
func (o Offsets) AsPartitions() map[string]map[int32]kgo.Offset {
	partitions := make(map[string]map[int32]kgo.Offset)
	for topic, partMap := range o {
		partitions[topic] = make(map[int32]kgo.Offset)
		for partition, offset := range partMap {
			partitions[topic][partition] = kgo.NewOffset().At(offset)
		}
	}
	return partitions
}
