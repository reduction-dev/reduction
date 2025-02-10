package kinesis

import (
	"context"
	"fmt"
	"log"
	"sync"

	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/connectors/kinesis/kinesispb"
	"reduction.dev/reduction/proto/workerpb"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/aws/aws-sdk-go-v2/aws"
)

type SourceReader struct {
	client         *Client
	streamARN      string
	assignedShards *shardList
}

type kinesisShard struct {
	id     string
	cursor string
}

func NewSourceReader(config SourceConfig) *SourceReader {
	if config.Client == nil {
		var err error
		config.Client, err = NewClient(&NewClientParams{
			Endpoint:    config.Endpoint,
			Region:      "us-east-2", // Match the region used in tests
			Credentials: aws.AnonymousCredentials{},
		})
		if err != nil {
			log.Fatalf("failed to create kinesis client: %s", err)
		}
	}

	return &SourceReader{
		client:         config.Client,
		streamARN:      config.StreamARN,
		assignedShards: newShardList(),
	}
}

// Poll the assigned Kinesis shards for records.
func (s *SourceReader) ReadEvents() ([][]byte, error) {
	// Wait until we have assigned splits
	s.assignedShards.ready.Wait()
	shard := s.assignedShards.next()

	records, err := s.client.ReadEvents(context.Background(), s.streamARN, shard.id, shard.cursor)
	if err != nil {
		return nil, fmt.Errorf("kinesis SourceReader ReadEvents: %w", err)
	}
	shard.cursor = *records.NextShardIterator

	events := make([][]byte, len(records.Records))
	for i, r := range records.Records {
		pbRecord := &kinesispb.Record{
			Data:      r.Data,
			Timestamp: timestamppb.New(*r.ApproximateArrivalTimestamp),
		}
		data, err := proto.Marshal(pbRecord)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal record: %w", err)
		}
		events[i] = data
	}

	return events, nil
}

func (s *SourceReader) SetSplits(splits []*workerpb.SourceSplit) error {
	shards := make([]*kinesisShard, len(splits))
	for i, split := range splits {
		shards[i] = &kinesisShard{
			id:     split.SplitId,
			cursor: string(split.Cursor),
		}
	}
	s.assignedShards.add(shards)
	return nil
}

func (s *SourceReader) Checkpoint() []byte {
	shards := make([]*kinesispb.Shard, len(s.assignedShards.shards))
	for i, s := range s.assignedShards.list() {
		shards[i] = &kinesispb.Shard{
			ShardId: s.id,
			Cursor:  s.cursor,
		}
	}
	snapshot := &kinesispb.Checkpoint{
		Shards: shards,
	}
	bs, err := proto.Marshal(snapshot)
	if err != nil {
		panic(err)
	}
	return bs
}

var _ connectors.SourceReader = (*SourceReader)(nil)

type shardList struct {
	shards []*kinesisShard
	mu     *sync.Mutex
	ready  *sync.WaitGroup
	index  int
}

func newShardList() *shardList {
	ready := &sync.WaitGroup{}
	ready.Add(1)

	return &shardList{
		mu:    &sync.Mutex{},
		ready: ready,
	}
}

func (l *shardList) add(shards []*kinesisShard) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Going from 0 to some marks the list ready
	if len(l.shards) == 0 && len(shards) != 0 {
		defer l.ready.Done()
	}
	l.shards = append(l.shards, shards...)
}

func (l *shardList) next() *kinesisShard {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Reset to zero if we're out of bounds
	if l.index >= len(l.shards) {
		l.index = 0
	}
	defer func() { l.index++ }()

	return l.shards[l.index]
}

func (l *shardList) list() []*kinesisShard {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.shards
}
