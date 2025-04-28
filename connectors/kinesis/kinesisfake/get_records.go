package kinesisfake

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"

	kinesistypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"reduction.dev/reduction/util/ptr"
)

type GetRecordsRequest struct {
	Limit         int
	ShardIterator string
	StreamARN     string
}

// GetRecordsResponse matches kinesis.GetRecordsOutput
type GetRecordsResponse struct {
	MillisBehindLatest *int64   `json:"MillisBehindLatest,omitempty"`
	NextShardIterator  *string  `json:"NextShardIterator,omitempty"`
	Records            []Record `json:"Records"`
}

// Record matches kinesistypes.Record
type Record struct {
	ApproximateArrivalTimestamp float64 `json:"ApproximateArrivalTimestamp,omitempty"`
	Data                        []byte  `json:"Data"`
	EncryptionType              string  `json:"EncryptionType,omitempty"`
	PartitionKey                string  `json:"PartitionKey"`
	SequenceNumber              string  `json:"SequenceNumber"`
}

func (f *Fake) getRecords(body []byte) (*GetRecordsResponse, error) {
	// Check if we have a simulated error
	if f.getRecordsError != nil {
		return nil, f.getRecordsError
	}

	var request GetRecordsRequest
	if err := json.Unmarshal(body, &request); err != nil {
		return nil, fmt.Errorf("decode GetRecords: %w", err)
	}

	stream := f.db.streams[streamNameFromARN(request.StreamARN)]
	if stream == nil {
		return nil, &kinesistypes.ResourceNotFoundException{
			Message: ptr.New(fmt.Sprintf("no stream %s", request.StreamARN)),
		}
	}

	// Parse the iterator
	parsedIterator, err := splitShardIterator(request.ShardIterator)
	if err != nil {
		return nil, fmt.Errorf("invalid shard iterator: %w", err)
	}

	if parsedIterator.shardID == "" {
		return nil, &kinesistypes.ResourceNotFoundException{
			Message: ptr.New("no shard " + request.ShardIterator),
		}
	}

	// Find the shard by index
	shardIndex := slices.IndexFunc(stream.shards, func(s *shard) bool {
		return s.id == parsedIterator.shardID
	})
	if shardIndex == -1 {
		return nil, &kinesistypes.ResourceNotFoundException{
			Message: ptr.New("no shard " + parsedIterator.shardID),
		}
	}
	shard := stream.shards[shardIndex]

	// Check if the iterator has expired
	if parsedIterator.timestamp <= f.iteratorsExpirationAt.Load() {
		return nil, &kinesistypes.ExpiredIteratorException{
			Message: ptr.New(fmt.Sprintf("Iterator %s has expired", request.ShardIterator)),
		}
	}

	// Get the records from the position
	var endPosition int
	if request.Limit != 0 {
		endPosition = parsedIterator.position + request.Limit
	} else {
		endPosition = parsedIterator.position + f.getRecordsLimit
	}
	endPosition = min(endPosition, len(shard.records))

	records := shard.records[parsedIterator.position:endPosition]
	responseRecords := make([]Record, len(records))
	for i, r := range records {
		responseRecords[i] = Record{
			ApproximateArrivalTimestamp: r.ApproximateArrivalTimestamp,
			Data:                        r.Data,
			PartitionKey:                r.PartitionKey,
			SequenceNumber:              strconv.Itoa(parsedIterator.position + i),
		}
	}
	slog.Info("getRecords", "shardID", parsedIterator.shardID, "position", parsedIterator.position, "endPosition", endPosition, "records", len(records))

	// Increment timestamp for the next iterator using atomic operations
	nextTimestamp := f.lastIteratorTimestamp.Add(1)

	// Next iterator is nil if we are at the end of the shard and the shard has
	// been superceeded by another child shard.
	var nextIterator *string
	if !(endPosition >= len(shard.records) && shard.isFinished) {
		nextIterator = ptr.New(shardIteratorFor(parsedIterator.shardID, nextTimestamp, endPosition))
	}

	return &GetRecordsResponse{
		NextShardIterator:  nextIterator,
		Records:            responseRecords,
		MillisBehindLatest: ptr.New(int64(0)),
	}, nil
}

type parsedIterator struct {
	shardID   string
	timestamp int64
	position  int
}

func splitShardIterator(iter string) (parsedIterator, error) {
	parts := strings.Split(iter, ":")
	if len(parts) != 3 {
		return parsedIterator{}, fmt.Errorf("invalid iterator format: %s", iter)
	}

	timestamp, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return parsedIterator{}, fmt.Errorf("invalid timestamp in iterator: %w", err)
	}

	pos, err := strconv.Atoi(parts[2])
	if err != nil {
		return parsedIterator{}, fmt.Errorf("invalid position in iterator: %w", err)
	}

	return parsedIterator{parts[0], timestamp, pos}, nil
}
