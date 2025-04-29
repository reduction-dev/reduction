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
	lastAssignedShardId     string // Track the last assigned shard ID
	splitTracker            *SplitTracker
	shardDiscoveryInterval  time.Duration
	shardDiscoveryTicker    *time.Ticker

	// splitsDidFinish signals that some splits were removed and new splits may
	// be available for assignment.
	splitsDidFinish chan struct{}

	// Context that tracks the lifetime of the source splitter.
	ctx context.Context

	// cancel function to cancel the context.
	cancel context.CancelFunc
}

func NewSourceSplitter(config SourceConfig, sourceRunnerIDs []string, hooks connectors.SourceSplitterHooks, errChan chan<- error) *SourceSplitter {
	// Set the default shard discovery interval to 10 seconds
	shardDiscoveryInterval := 10 * time.Second
	if config.ShardDiscoveryInterval != 0 {
		shardDiscoveryInterval = config.ShardDiscoveryInterval
	}

	return &SourceSplitter{
		client:                 config.NewKinesisClient(),
		streamARN:              config.StreamARN,
		cursors:                make(map[string]string),
		errChan:                errChan,
		hooks:                  hooks,
		sourceRunnerIDs:        sourceRunnerIDs,
		splitTracker:           NewSplitTracker(),
		shardDiscoveryInterval: shardDiscoveryInterval,
		splitsDidFinish:        make(chan struct{}, 1),
		ctx:                    context.Background(),
	}
}

// Start initializes the SourceSplitter, optionally loading a checkpoint.
func (s *SourceSplitter) Start(ckpt *snapshotpb.SourceCheckpoint) error {
	ctx, cancel := context.WithCancel(context.Background())
	s.ctx = ctx
	s.cancel = cancel

	// Load the split state
	var pendingShardIDs []string
	for _, splitState := range ckpt.GetSplitStates() {
		var split kinesispb.Shard
		if err := proto.Unmarshal(splitState, &split); err != nil {
			return fmt.Errorf("kinesis.SourceSplitter failed to unmarshal split state: %w", err)
		}
		s.cursors[split.ShardId] = split.Cursor
		pendingShardIDs = append(pendingShardIDs, split.ShardId)
	}

	// Load the splitter state
	var splitterState kinesispb.SplitterState
	if err := proto.Unmarshal(ckpt.GetSplitterState(), &splitterState); err != nil {
		return fmt.Errorf("kinesis.SourceSplitter failed to unmarshal splitter state: %w", err)
	}
	s.lastAssignedShardId = splitterState.LastAssignedShardId

	shardIDs, err := s.discoverShards(ctx, s.lastAssignedShardId)
	if err != nil {
		return fmt.Errorf("kinesis.SourceSplitter failed to discover shards: %w", err)
	}
	pendingShardIDs = append(pendingShardIDs, shardIDs...)

	s.assignShards(ctx, pendingShardIDs)

	s.shardDiscoveryTicker = time.NewTicker(s.shardDiscoveryInterval)
	go s.processShardAssignment(ctx)
	return nil
}

func (s *SourceSplitter) processShardAssignment(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.shardDiscoveryTicker.C:
			// periodically discover shards and assign them to source runners
			splitIDs, err := s.discoverShards(s.ctx, s.lastAssignedShardId)
			if err != nil {
				s.errChan <- fmt.Errorf("kinesis.SourceSplitter failed to discover shards: %w", err)
				return
			}
			s.assignShards(s.ctx, splitIDs)
		case <-s.splitsDidFinish:
			// When splits finish, their child splits may become available
			pending := s.splitTracker.AvailableSplits()
			s.assignShards(s.ctx, pending)
		}
	}
}

func (s *SourceSplitter) IsSourceSplitter() {}

func (s *SourceSplitter) NotifySplitsFinished(sourceRunnerID string, splitIDs []string) {
	s.splitTracker.RemoveSplits(splitIDs)
	s.splitsDidFinish <- struct{}{}
}

func (s *SourceSplitter) Close() error {
	if s.shardDiscoveryTicker != nil {
		s.shardDiscoveryTicker.Stop()
	}
	if s.cancel != nil {
		s.cancel()
	}
	return nil
}

// Checkpoint returns a snapshot of the splitter's state for checkpointing.
func (s *SourceSplitter) Checkpoint() []byte {
	bs, err := proto.Marshal(&kinesispb.SplitterState{
		LastAssignedShardId: s.lastAssignedShardId, // Use lastAssignedShardId for checkpointing
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

// discoverShards fetches new shards and returns any shards ready for assignment.
func (s *SourceSplitter) discoverShards(ctx context.Context, lastAssignedShardID string) ([]string, error) {
	shards, err := s.listAllShards(ctx, lastAssignedShardID)
	if err != nil {
		return nil, fmt.Errorf("kinesis.SourceSplitter failed to list shards: %w", err)
	}
	s.splitTracker.AddSplits(shards)
	return s.splitTracker.AvailableSplits(), nil
}

// assignShards assings the list of shards to the source runners. It tracks the
// assignmets in internal state.
func (s *SourceSplitter) assignShards(ctx context.Context, shardIDs []string) {
	if len(shardIDs) == 0 {
		return
	}

	assignments := make(map[string][]*workerpb.SourceSplit, len(s.sourceRunnerIDs))
	shardIDGroups := sliceu.Partition(shardIDs, len(s.sourceRunnerIDs))
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
	s.splitTracker.TrackAssigned(shardIDs)

	// Update lastAssignedShardId to the highest assigned shard ID (if any)
	if len(shardIDs) > 0 {
		s.lastAssignedShardId = shardIDs[len(shardIDs)-1]
	}
}

var _ connectors.SourceSplitter = (*SourceSplitter)(nil)
