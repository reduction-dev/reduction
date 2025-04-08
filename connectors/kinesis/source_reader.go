package kinesis

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	kinesistypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	protocol "reduction.dev/reduction-protocol/kinesispb"
	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/connectors/kinesis/kinesispb"
	"reduction.dev/reduction/proto/workerpb"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type SourceReader struct {
	client         *Client
	streamARN      string
	assignedShards *shardList
}

type kinesisShard struct {
	id             string
	shardIterator  string
	sequenceNumber string
}

func NewSourceReader(config SourceConfig) *SourceReader {
	if config.Client == nil {
		var err error
		config.Client, err = NewClient(&NewClientParams{
			Endpoint: config.Endpoint,
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

	// Read from shards round-robin
	shard := s.assignedShards.next()

	// If we don't have a shardIterator yet, we need to get one
	if shard.shardIterator == "" {
		if shard.sequenceNumber == "" {
			fmt.Println("starting from trim horizon")
			// If we don't have a sequence number, we start from the trim horizon
			it, err := s.client.GetShardIterator(context.Background(), &kinesis.GetShardIteratorInput{
				StreamARN:         &s.streamARN,
				ShardId:           &shard.id,
				ShardIteratorType: kinesistypes.ShardIteratorTypeTrimHorizon,
			})
			if err != nil {
				return nil, fmt.Errorf("kinesis SourceReader.ReadEvents getting shard iterator: %w", sourceErrorFrom(err))
			}
			shard.shardIterator = it
		} else {
			// If we do have a sequence number, we start after that
			fmt.Println("starting from sequence number: ", shard.sequenceNumber)
			it, err := s.client.GetShardIterator(context.Background(), &kinesis.GetShardIteratorInput{
				StreamARN:              &s.streamARN,
				ShardId:                &shard.id,
				ShardIteratorType:      kinesistypes.ShardIteratorTypeAfterSequenceNumber,
				StartingSequenceNumber: &shard.sequenceNumber,
			})
			if err != nil {
				return nil, fmt.Errorf("kinesis SourceReader.ReadEvents getting shard iterator: %w", sourceErrorFrom(err))
			}
			shard.shardIterator = it
		}
	}

	records, err := s.client.GetRecords(context.Background(), &kinesis.GetRecordsInput{
		StreamARN:     &s.streamARN,
		ShardIterator: &shard.shardIterator,
	})
	if err != nil {
		return nil, fmt.Errorf("kinesis SourceReader.ReadEvents: %w", sourceErrorFrom(err))
	}

	// Store the next shard iterator for subsequent reads
	shard.shardIterator = *records.NextShardIterator

	// If we have records, update the sequence number with the last one
	if len(records.Records) > 0 {
		lastRecord := records.Records[len(records.Records)-1]
		if lastRecord.SequenceNumber != nil {
			shard.sequenceNumber = *lastRecord.SequenceNumber
		}
	}

	// Convert the records to protobuf format
	events := make([][]byte, len(records.Records))
	for i, r := range records.Records {
		pbRecord := &protocol.Record{
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
			id:             split.SplitId,
			sequenceNumber: string(split.Cursor),
		}
	}
	s.assignedShards.add(shards)
	return nil
}

func (s *SourceReader) Checkpoint() []byte {
	shards := make([]*kinesispb.Shard, len(s.assignedShards.shards))
	for i, shard := range s.assignedShards.list() {
		shards[i] = &kinesispb.Shard{
			ShardId: shard.id,
			Cursor:  shard.sequenceNumber,
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

func sourceErrorFrom(err error) *connectors.SourceError {
	var accessDenied *kinesistypes.AccessDeniedException
	if errors.As(err, &accessDenied) {
		return connectors.NewTerminalError(err)
	}

	var invalidArgument *kinesistypes.InvalidArgumentException
	if errors.As(err, &invalidArgument) {
		return connectors.NewTerminalError(err)
	}

	// Default to retryable for unknown errors
	return connectors.NewRetryableError(err)
}
