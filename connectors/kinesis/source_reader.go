package kinesis

import (
	"context"
	"errors"
	"fmt"
	"log"

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
	assignedShards []*assignedShard
	shardIndex     int
	hooks          connectors.SourceReaderHooks
}

type assignedShard struct {
	// Kinesis Provided Shard ID
	id string
	// The continuation token for the next read. Only lasts 5m and then expires.
	shardIterator string
	// The sequence number of the last record read from this shard.
	sequenceNumber string
	// Flag to indicate if the shard has reached the end of input.
	isFinished bool
}

func NewSourceReader(config SourceConfig, hooks connectors.SourceReaderHooks) *SourceReader {
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
		client:    config.Client,
		streamARN: config.StreamARN,
		hooks:     hooks,
	}
}

// Poll the assigned Kinesis shards for records.
func (s *SourceReader) ReadEvents() ([][]byte, error) {
	// No-op if there are no shards to read
	if len(s.assignedShards) == 0 {
		return [][]byte{}, nil
	}

	// Find the next non-finished shard, starting from current index
	startIdx := s.shardIndex
	found := false
	var shard *assignedShard
	for i := range s.assignedShards {
		idx := (startIdx + i) % len(s.assignedShards)
		if !s.assignedShards[idx].isFinished {
			shard = s.assignedShards[idx]
			s.shardIndex = (idx + 1) % len(s.assignedShards)
			found = true
			break
		}
	}
	if !found {
		// All shards are finished
		return [][]byte{}, nil
	}

	// If we don't have a shardIterator yet, we need to get one
	if shard.shardIterator == "" {
		if err := s.refreshShardIterator(shard); err != nil {
			return nil, err
		}
	}

	records, err := s.client.GetRecords(context.Background(), &kinesis.GetRecordsInput{
		StreamARN:     &s.streamARN,
		ShardIterator: &shard.shardIterator,
	})

	// Check for expired iterator and refresh if needed
	var expiredIterator *kinesistypes.ExpiredIteratorException
	if errors.As(err, &expiredIterator) {
		if err := s.refreshShardIterator(shard); err != nil {
			return nil, err
		}

		// Try again with the new iterator
		records, err = s.client.GetRecords(context.Background(), &kinesis.GetRecordsInput{
			StreamARN:     &s.streamARN,
			ShardIterator: &shard.shardIterator,
		})
	}

	if err != nil {
		return nil, fmt.Errorf("kinesis SourceReader.ReadEvents: %w", sourceErrorFrom(err))
	}

	// Handle finished shards
	if records.NextShardIterator == nil {
		shard.isFinished = true
		s.hooks.NotifySplitsFinished([]string{shard.id})
	} else {
		// Update shard state for continued reading
		shard.shardIterator = *records.NextShardIterator
		if len(records.Records) > 0 {
			lastRecord := records.Records[len(records.Records)-1]
			if lastRecord.SequenceNumber != nil {
				shard.sequenceNumber = *lastRecord.SequenceNumber
			}
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

func (s *SourceReader) AssignSplits(splits []*workerpb.SourceSplit) error {
	for _, split := range splits {
		s.assignedShards = append(s.assignedShards, &assignedShard{
			id:             split.SplitId,
			sequenceNumber: string(split.Cursor),
		})
	}
	return nil
}

func (s *SourceReader) Checkpoint() []byte {
	shards := make([]*kinesispb.Shard, len(s.assignedShards))
	for i, shard := range s.assignedShards {
		shards[i] = &kinesispb.Shard{
			ShardId:  shard.id,
			Cursor:   shard.sequenceNumber,
			Finished: shard.isFinished,
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

func (s *SourceReader) refreshShardIterator(shard *assignedShard) error {
	var iteratorType kinesistypes.ShardIteratorType
	var sequenceNumber *string

	if shard.sequenceNumber == "" {
		iteratorType = kinesistypes.ShardIteratorTypeTrimHorizon
	} else {
		iteratorType = kinesistypes.ShardIteratorTypeAfterSequenceNumber
		sequenceNumber = &shard.sequenceNumber
	}

	iterator, err := s.client.GetShardIterator(context.Background(), &kinesis.GetShardIteratorInput{
		StreamARN:              &s.streamARN,
		ShardId:                &shard.id,
		ShardIteratorType:      iteratorType,
		StartingSequenceNumber: sequenceNumber,
	})
	if err != nil {
		return fmt.Errorf("kinesis SourceReader.refreshShardIterator getting shard iterator: %w", sourceErrorFrom(err))
	}

	shard.shardIterator = iterator
	return nil
}

var _ connectors.SourceReader = (*SourceReader)(nil)

func sourceErrorFrom(err error) *connectors.SourceError {
	switch {
	case errors.As(err, new(*kinesistypes.AccessDeniedException)):
		return connectors.NewTerminalError(err)
	case errors.As(err, new(*kinesistypes.InvalidArgumentException)):
		return connectors.NewTerminalError(err)
	default:
		// All other errors are retryable
		return connectors.NewRetryableError(err)
	}
}
