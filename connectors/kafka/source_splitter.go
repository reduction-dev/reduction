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
	"reduction.dev/reduction/proto/snapshotpb"
	"reduction.dev/reduction/proto/workerpb"
	"reduction.dev/reduction/util/sliceu"
)

type SourceSplitter struct {
	client          *kadm.Client
	topics          []string
	offsets         map[string]map[int32]kgo.Offset // Map topic -> partition ID -> offset
	sourceRunnerIDs []string
	hooks           connectors.SourceSplitterHooks
	errChan         chan<- error
}

// NewSourceSplitter creates a Kafka source splitter
func NewSourceSplitter(config SourceConfig, sourceRunnerIDs []string, hooks connectors.SourceSplitterHooks, errChan chan<- error) (*SourceSplitter, error) {
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
		client:          kadm.NewClient(config.Client),
		topics:          config.Topics,
		offsets:         make(map[string]map[int32]kgo.Offset),
		sourceRunnerIDs: sourceRunnerIDs,
		hooks:           hooks,
		errChan:         errChan,
	}, nil
}

func (s *SourceSplitter) Start() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	meta, err := s.client.Metadata(ctx, s.topics...)
	if err != nil {
		s.errChan <- fmt.Errorf("kafka.SourceSplitter failed to fetch metadata: %w", err)
		return
	}

	type split struct {
		Topic     string
		Partition int32
	}
	var splits []split
	for _, topicMeta := range meta.Topics {
		if topicMeta.Err != nil {
			s.errChan <- fmt.Errorf("kafka.SourceSplitter: error in topic metadata for %s: %w", topicMeta.Topic, topicMeta.Err)
			return
		}
		for _, partMeta := range topicMeta.Partitions {
			splits = append(splits, split{Topic: topicMeta.Topic, Partition: partMeta.Partition})
		}
	}

	assignments := make(map[string][]*workerpb.SourceSplit, len(s.sourceRunnerIDs))
	splitGroups := sliceu.Partition(splits, len(s.sourceRunnerIDs))
	for i, id := range s.sourceRunnerIDs {
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

	s.hooks.AssignSplits(assignments)
}

func (s *SourceSplitter) LoadCheckpoint(ckpt *snapshotpb.SourceCheckpoint) error {
	for _, cpData := range ckpt.SplitStates {
		var ss kafkapb.SplitState
		if err := proto.Unmarshal(cpData, &ss); err != nil {
			return fmt.Errorf("failed to unmarshal checkpoint: %w", err)
		}
		if s.offsets[ss.Topic] == nil {
			s.offsets[ss.Topic] = make(map[int32]kgo.Offset)
		}
		s.offsets[ss.Topic][ss.Parition] = kgo.NewOffset().At(ss.Offset)
	}
	return nil
}

func (s *SourceSplitter) IsSourceSplitter() {}

func (s *SourceSplitter) Close() error { return nil }

func (s *SourceSplitter) NotifySplitsFinished(sourceRunnerID string, splitIDs []string) {}

func (s *SourceSplitter) Checkpoint() []byte { return nil }

var _ connectors.SourceSplitter = (*SourceSplitter)(nil)
