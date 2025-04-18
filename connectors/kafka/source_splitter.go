package kafka

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/connectors/kafka/kafkapb"
	"reduction.dev/reduction/proto/workerpb"
	"reduction.dev/reduction/util/sliceu"
)

type SourceSplitter struct {
	client  *kadm.Client
	topics  []string
	offsets map[string]map[int32]kgo.Offset // Map topic -> partition ID -> offset
}

// NewSourceSplitter creates a Kafka source splitter
func NewSourceSplitter(config SourceConfig) (*SourceSplitter, error) {
	if config.Client == nil {
		var err error
		config.Client, err = kgo.NewClient(
			kgo.ConsumerGroup(config.ConsumerGroup),
			kgo.SeedBrokers(config.Brokers...),
			kgo.ConsumeTopics(config.Topics...),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create Kafka client: %w", err)
		}
	}

	return &SourceSplitter{
		client:  kadm.NewClient(config.Client),
		topics:  config.Topics,
		offsets: make(map[string]map[int32]kgo.Offset),
	}, nil
}

func (s *SourceSplitter) AssignSplits(ids []string) (map[string][]*workerpb.SourceSplit, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Fetch metadata for all topics
	meta, err := s.client.Metadata(ctx, s.topics...)
	if err != nil {
		return nil, fmt.Errorf("kafka.SourceSplitter failed to fetch metadata: %w", err)
	}

	type split struct {
		Topic     string
		Partition int32
	}
	var splits []split
	for _, topicMeta := range meta.Topics {
		if topicMeta.Err != nil {
			return nil, fmt.Errorf("kafka.SourceSplitter: error in topic metadata for %s: %w", topicMeta.Topic, topicMeta.Err)
		}
		for _, partMeta := range topicMeta.Partitions {
			splits = append(splits, split{Topic: topicMeta.Topic, Partition: partMeta.Partition})
		}
	}

	assignments := make(map[string][]*workerpb.SourceSplit, len(ids))
	splitGroups := sliceu.Partition(splits, len(ids))
	for i, id := range ids {
		group := splitGroups[i]
		assignments[id] = make([]*workerpb.SourceSplit, len(group))
		for j, split := range group {
			splitID := fmt.Sprintf("%s:%d", split.Topic, split.Partition)
			var offset kgo.Offset
			if s.offsets != nil {
				if topicOffsets, ok := s.offsets[split.Topic]; ok {
					offset = topicOffsets[split.Partition]
				}
			}
			epochOffset := offset.EpochOffset().Offset
			cursor := []byte(strconv.FormatInt(epochOffset, 10))
			assignments[id][j] = &workerpb.SourceSplit{
				SourceId: "kafka-source",
				SplitId:  splitID,
				Cursor:   cursor,
			}
		}
	}
	return assignments, nil
}

func (s *SourceSplitter) LoadCheckpoints(checkpoints [][]byte) error {
	for _, cpData := range checkpoints {
		var checkpoint kafkapb.Checkpoint
		if err := proto.Unmarshal(cpData, &checkpoint); err != nil {
			return fmt.Errorf("failed to unmarshal checkpoint: %w", err)
		}
		for _, topic := range checkpoint.Topics {
			if s.offsets[topic.Name] == nil {
				s.offsets[topic.Name] = make(map[int32]kgo.Offset)
			}
			for _, part := range topic.Partitions {
				s.offsets[topic.Name][part.Id] = kgo.NewOffset().At(part.Offset)
			}
		}
	}
	return nil
}

func (s *SourceSplitter) IsSourceSplitter() {}

func (s *SourceSplitter) Start() {}

func (s *SourceSplitter) Close() error { return nil }

var _ connectors.SourceSplitter = (*SourceSplitter)(nil)
