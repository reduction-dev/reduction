package kinesis

import (
	"context"
	"fmt"
	"log"
	"time"

	"google.golang.org/protobuf/proto"
	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/connectors/kinesis/kinesispb"
	"reduction.dev/reduction/proto/workerpb"
	"reduction.dev/reduction/util/sliceu"
)

type SourceSplitter struct {
	client          *Client
	streamARN       string
	shardIDs        []string
	cursors         map[string]string
	sourceRunnerIDs []string
	errChan         chan<- error
	hooks           connectors.SourceSplitterHooks
}

func NewSourceSplitter(config SourceConfig, sourceRunnerIDs []string, hooks connectors.SourceSplitterHooks, errChan chan<- error) *SourceSplitter {
	client := config.Client
	if client == nil {
		var err error
		client, err = NewClient(&NewClientParams{
			Endpoint: config.Endpoint,
		})
		if err != nil {
			log.Fatalf("Failed to create Kinesis Client: %s", err)
		}
	}

	return &SourceSplitter{
		client:          client,
		streamARN:       config.StreamARN,
		cursors:         make(map[string]string),
		errChan:         errChan,
		hooks:           hooks,
		sourceRunnerIDs: sourceRunnerIDs,
	}
}

// LoadCheckpoints receives checkpoint data created by source readers
// and loads cursor data for each shard id.
func (s *SourceSplitter) LoadCheckpoints(checkpoints [][]byte) error {
	for _, cpData := range checkpoints {
		var checkpoint kinesispb.Checkpoint
		if err := proto.Unmarshal(cpData, &checkpoint); err != nil {
			return err
		}
		for _, shard := range checkpoint.Shards {
			s.cursors[shard.ShardId] = shard.Cursor
		}
	}
	return nil
}

func (s *SourceSplitter) Start() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	shards, err := s.client.ListShards(ctx, s.streamARN)
	if err != nil {
		s.errChan <- fmt.Errorf("kinesis.SourceSplitter failed to list shards: %w", err)
	}
	s.shardIDs = make([]string, len(shards))
	for i, shard := range shards {
		s.shardIDs[i] = *shard.ShardId
	}

	assignments := make(map[string][]*workerpb.SourceSplit, len(s.sourceRunnerIDs))
	shardIDGroups := sliceu.Partition(s.shardIDs, len(s.sourceRunnerIDs))
	for i, id := range s.sourceRunnerIDs {
		assignedShards := shardIDGroups[i]
		assignments[id] = make([]*workerpb.SourceSplit, len(assignedShards))
		for i, shardID := range assignedShards {
			assignments[id][i] = &workerpb.SourceSplit{
				SourceId: "tbd",
				SplitId:  shardID,
				Cursor:   []byte(s.cursors[shardID]),
			}
		}
	}

	s.hooks.AssignSplits(assignments)
}

func (s *SourceSplitter) IsSourceSplitter() {}

func (s *SourceSplitter) NotifySplitsFinished(sourceRunnerID string, splitIDs []string) {}

func (s *SourceSplitter) Close() error { return nil }

var _ connectors.SourceSplitter = (*SourceSplitter)(nil)
