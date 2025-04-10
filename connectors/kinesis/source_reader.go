package kinesis

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	kinesistypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/aws/smithy-go"
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
	assignedShards []*kinesisShard
	shardIndex     int
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
		client:    config.Client,
		streamARN: config.StreamARN,
	}
}

// Poll the assigned Kinesis shards for records.
func (s *SourceReader) ReadEvents() ([][]byte, error) {
	// No-op if there are no shards to read
	if len(s.assignedShards) == 0 {
		return [][]byte{}, nil
	}

	// Read from one shard, picking round-robin
	shard := s.assignedShards[s.shardIndex]
	s.shardIndex = (s.shardIndex + 1) % len(s.assignedShards)

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

	// Update shard state
	shard.shardIterator = *records.NextShardIterator
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
	s.assignedShards = shards
	return nil
}

func (s *SourceReader) Checkpoint() []byte {
	shards := make([]*kinesispb.Shard, len(s.assignedShards))
	for i, shard := range s.assignedShards {
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

func (s *SourceReader) refreshShardIterator(shard *kinesisShard) error {
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
	kinesisErrToConnectorErr := map[smithy.APIError]func(error) *connectors.SourceError{
		&kinesistypes.AccessDeniedException{}:    connectors.NewTerminalError,
		&kinesistypes.InvalidArgumentException{}: connectors.NewTerminalError,
	}

	// Check if the error is a terminal error
	for kinesisErr, connectorErr := range kinesisErrToConnectorErr {
		if errors.As(err, &kinesisErr) {
			return connectorErr(err)
		}
	}

	// Default to retryable for unknown errors
	return connectors.NewRetryableError(err)
}
