package kinesis

import (
	"context"
	"fmt"
	"time"

	awskinesis "github.com/aws/aws-sdk-go-v2/service/kinesis"
	awstypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"google.golang.org/protobuf/proto"
	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/connectors/kinesis/kinesispb"
	"reduction.dev/reduction/proto/snapshotpb"
	"reduction.dev/reduction/proto/workerpb"
	"reduction.dev/reduction/util/sliceu"
)

type SourceSplitter struct {
	client                  *awskinesis.Client
	streamARN               string
	shardsPendingAssignment []string
	cursors                 map[string]string
	sourceRunnerIDs         []string
	errChan                 chan<- error
	hooks                   connectors.SourceSplitterHooks
	LastSeenShardId         string
}

func NewSourceSplitter(config SourceConfig, sourceRunnerIDs []string, hooks connectors.SourceSplitterHooks, errChan chan<- error) *SourceSplitter {
	return &SourceSplitter{
		client:          config.NewKinesisClient(),
		streamARN:       config.StreamARN,
		cursors:         make(map[string]string),
		errChan:         errChan,
		hooks:           hooks,
		sourceRunnerIDs: sourceRunnerIDs,
	}
}

// LoadCheckpoints receives checkpoint data created by source readers
// and loads cursor data for each shard id.
func (s *SourceSplitter) LoadCheckpoint(ckpt *snapshotpb.SourceCheckpoint) error {
	// Load the split state
	for _, splitState := range ckpt.SplitStates {
		var split kinesispb.Shard
		if err := proto.Unmarshal(splitState, &split); err != nil {
			return err
		}
		s.cursors[split.ShardId] = split.Cursor
		s.shardsPendingAssignment = append(s.shardsPendingAssignment, split.ShardId)
	}

	// Load the splitter state
	var splitterState kinesispb.SplitterState
	err := proto.Unmarshal(ckpt.SplitterState, &splitterState)
	if err != nil {
		return err
	}
	s.LastSeenShardId = splitterState.LastSeenShardId

	return nil
}

func (s *SourceSplitter) Start() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	shards, err := s.listAllShards(ctx, s.LastSeenShardId)
	if err != nil {
		s.errChan <- fmt.Errorf("kinesis.SourceSplitter failed to list shards: %w", err)
		return
	}
	for _, shard := range shards {
		s.shardsPendingAssignment = append(s.shardsPendingAssignment, *shard.ShardId)
	}
	if len(s.shardsPendingAssignment) > 0 {
		s.LastSeenShardId = s.shardsPendingAssignment[len(s.shardsPendingAssignment)-1]
	}

	assignments := make(map[string][]*workerpb.SourceSplit, len(s.sourceRunnerIDs))
	shardIDGroups := sliceu.Partition(s.shardsPendingAssignment, len(s.sourceRunnerIDs))
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
	s.shardsPendingAssignment = nil
}

func (s *SourceSplitter) IsSourceSplitter() {}

func (s *SourceSplitter) NotifySplitsFinished(sourceRunnerID string, splitIDs []string) {}

func (s *SourceSplitter) Close() error { return nil }

// Checkpoint returns a snapshot of the splitter's state for checkpointing.
func (s *SourceSplitter) Checkpoint() []byte {
	bs, err := proto.Marshal(&kinesispb.SplitterState{
		LastSeenShardId: s.LastSeenShardId,
	})
	if err != nil {
		panic(err)
	}
	return bs
}

// listAllShards paginates through all shards for a given streamARN, starting after exclusiveStartShardID.
func (s *SourceSplitter) listAllShards(ctx context.Context, exclusiveStartShardID string) ([]awstypes.Shard, error) {
	var shards []awstypes.Shard
	var nextToken *string
	for {
		input := &awskinesis.ListShardsInput{StreamARN: &s.streamARN}
		if nextToken != nil {
			input.NextToken = nextToken
		} else if exclusiveStartShardID != "" {
			input.ExclusiveStartShardId = &exclusiveStartShardID
		}
		out, err := s.client.ListShards(ctx, input)
		if err != nil {
			return nil, err
		}
		shards = append(shards, out.Shards...)
		if out.NextToken == nil {
			break
		}
		nextToken = out.NextToken
	}
	return shards, nil
}

var _ connectors.SourceSplitter = (*SourceSplitter)(nil)
