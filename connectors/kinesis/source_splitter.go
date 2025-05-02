package kinesis

import (
	"context"
	"fmt"
	"math/big"
	"time"

	awskinesis "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"google.golang.org/protobuf/proto"
	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/connectors/kinesis/kinesispb"
	"reduction.dev/reduction/proto/snapshotpb"
	"reduction.dev/reduction/proto/workerpb"
)

var (
	bigTwo     = big.NewInt(2)
	hashKeyMax = new(big.Int).Exp(bigTwo, big.NewInt(128), nil)
)

type SourceSplitter struct {
	client                 *awskinesis.Client
	streamARN              string
	cursors                map[string]string
	sourceRunnerIDs        []string
	errChan                chan<- error
	hooks                  connectors.SourceSplitterHooks
	splitTracker           *SplitTracker
	shardDiscoveryInterval time.Duration
	shardDiscoveryTicker   *time.Ticker

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

	var pendingShards []SourceSplitterShard

	// Load the splitter state
	var splitterState kinesispb.SplitterState
	if err := proto.Unmarshal(ckpt.GetSplitterState(), &splitterState); err != nil {
		return fmt.Errorf("kinesis.SourceSplitter failed to unmarshal splitter state: %w", err)
	}

	// Build a list of shards that need to be assigned
	pendingShards = make([]SourceSplitterShard, len(splitterState.AssignedShards))
	for i, shard := range splitterState.GetAssignedShards() {
		pendingShards[i] = newSourceSplitterShardFromProto(shard)
	}
	s.splitTracker.LoadSplits(pendingShards, splitterState.LastAssignedShardId)

	// Load the split states to get the cursors
	for _, splitState := range ckpt.GetSplitStates() {
		var split kinesispb.Shard
		if err := proto.Unmarshal(splitState, &split); err != nil {
			return fmt.Errorf("kinesis.SourceSplitter failed to unmarshal split state: %w", err)
		}
		s.cursors[split.ShardId] = split.Cursor
	}

	// Include newly discovered shards for assignment
	err := s.discoverShards(ctx, s.splitTracker.LastAssignedSplitID)
	if err != nil {
		return fmt.Errorf("kinesis.SourceSplitter failed to discover shards: %w", err)
	}
	pendingShards = append(pendingShards, s.splitTracker.AvailableSplits()...)

	// Do the initial split assignment
	s.assignShards(ctx, pendingShards)

	// Setup background shard assignment
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
			err := s.discoverShards(s.ctx, s.splitTracker.LastAssignedSplitID)
			if err != nil {
				s.errChan <- fmt.Errorf("kinesis.SourceSplitter failed to discover shards: %w", err)
				return
			}
			pending := s.splitTracker.AvailableSplits()
			s.assignShards(s.ctx, pending)
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
	splits := s.splitTracker.AssignedSplits()
	pbShards := make([]*kinesispb.SourceSplitterShard, len(splits))
	for i, shard := range splits {
		pbShards[i] = shard.toProto()
	}

	bs, err := proto.Marshal(&kinesispb.SplitterState{
		AssignedShards:      pbShards,
		LastAssignedShardId: s.splitTracker.LastAssignedSplitID,
	})
	if err != nil {
		panic(err)
	}
	return bs
}

// listAllShards paginates through all shards for a given streamARN, starting after exclusiveStartShardID.
func (s *SourceSplitter) listAllShards(ctx context.Context, exclusiveStartShardID string) ([]SourceSplitterShard, error) {
	var shards []SourceSplitterShard
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
		for _, shard := range out.Shards {
			shards = append(shards, newSourceSplitterShardFromKinesis(shard))
		}
		if out.NextToken == nil {
			break
		}
		nextToken = out.NextToken
	}
	return shards, nil
}

// discoverShards fetches new shards and returns any shards ready for assignment.
func (s *SourceSplitter) discoverShards(ctx context.Context, lastAssignedShardID string) error {
	shards, err := s.listAllShards(ctx, lastAssignedShardID)
	if err != nil {
		return fmt.Errorf("kinesis.SourceSplitter failed to list shards: %w", err)
	}
	s.splitTracker.AddSplits(shards)
	return nil
}

// assignShards assings the list of shards to the source runners. It tracks the
// assignmets in internal state.
func (s *SourceSplitter) assignShards(ctx context.Context, shards []SourceSplitterShard) {
	if len(shards) == 0 {
		return
	}

	// Build a map from runner ID to assigned splits
	assignments := make(map[string][]*workerpb.SourceSplit, len(s.sourceRunnerIDs))
	for _, shard := range shards {
		idx := uniformlyAssignShard(shard.HashKeyRange, len(s.sourceRunnerIDs))
		runnerID := s.sourceRunnerIDs[idx]
		assignments[runnerID] = append(assignments[runnerID], &workerpb.SourceSplit{
			SourceId: "tbd",
			SplitId:  shard.ShardID,
			Cursor:   []byte(s.cursors[shard.ShardID]),
		})
	}

	s.hooks.AssignSplits(assignments)
	s.splitTracker.TrackAssigned(shards)
}

var _ connectors.SourceSplitter = (*SourceSplitter)(nil)

// uniformlyAssignShard returns the index of the sourceRunnerIDs to assign the
// shard to, based on the shard hash key range.
//
// The algorithm is:
//  1. Calculate the midpoint of the hash key range: (start + end) / 2
//  2. Calculate a ratio representing the position of the midpoint in the hash key range:
//     midpoint / 2^128
//  3. Multiply the fraction by the number of runners.
func uniformlyAssignShard(hashKeyRange HashKeyRange, numRunners int) int {
	// Find absolute midpoint of a shard hash key range
	mid := new(big.Int).Add(hashKeyRange.Start, hashKeyRange.End)
	mid.Div(mid, bigTwo)

	// Calculate the relative position (0 to 1) in the key space.
	pos := new(big.Rat).SetFrac(mid, hashKeyMax)

	// Calculate the index of the runner to assign the shard to.
	idxRat := pos.Mul(pos, new(big.Rat).SetInt64(int64(numRunners)))
	idxFloat, _ := idxRat.Float64()
	idx := int(idxFloat)

	// Ensure the index is within bounds in case of floating point imprecision.
	return min(idx, numRunners-1)
}
