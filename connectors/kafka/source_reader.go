package kafka

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
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
	return &SourceReader{client: config.Client}
}

func (s *SourceReader) SetSplits(splits []*workerpb.SourceSplit) error {
	offsets := NewOffsets()
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
		offsets.Set(topic, int32(partition), offset)
	}
	s.offsets = offsets

	s.client.AddConsumePartitions(s.offsets.AsPartitions())
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
		s.offsets.Set(record.Topic, record.Partition, record.Offset)
		events = append(events, record.Value)
	}
	if err := fetches.Err(); err != nil {
		return nil, fmt.Errorf("kafka SourceReader.ReadEvents: %w", connectors.NewRetryableError(err))
	}
	return events, nil
}

func (s *SourceReader) Checkpoint() []byte {
	checkpoint := &kafkapb.Checkpoint{}
	for topicName, partMap := range s.offsets {
		partitions := make([]*kafkapb.Partition, 0, len(partMap))
		for partition, offset := range partMap {
			partitions = append(partitions, &kafkapb.Partition{
				Id:     partition,
				Offset: offset,
			})
		}
		checkpoint.Topics = append(checkpoint.Topics, &kafkapb.Topic{
			Name:       topicName,
			Partitions: partitions,
		})
	}
	bs, err := proto.Marshal(checkpoint)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal checkpoint: %v", err))
	}
	return bs
}

var _ connectors.SourceReader = (*SourceReader)(nil)

type Offsets map[string]map[int32]int64

func NewOffsets() Offsets {
	return make(Offsets)
}

// EnsurePartition ensures the topic/partition map exists and returns it.
func (o Offsets) EnsurePartition(topic string, partition int32) {
	if o[topic] == nil {
		o[topic] = make(map[int32]int64)
	}
}

// Set sets the offset for a topic/partition.
func (o Offsets) Set(topic string, partition int32, offset int64) {
	o.EnsurePartition(topic, partition)
	o[topic][partition] = offset
}

// Get returns the offset and true if present.
func (o Offsets) Get(topic string, partition int32) (int64, bool) {
	partMap, ok := o[topic]
	if !ok {
		return 0, false
	}
	offset, ok := partMap[partition]
	return offset, ok
}

// ForEach calls fn for each topic/partition/offset.
func (o Offsets) ForEach(fn func(topic string, partition int32, offset int64)) {
	for topic, partMap := range o {
		for partition, offset := range partMap {
			fn(topic, partition, offset)
		}
	}
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
