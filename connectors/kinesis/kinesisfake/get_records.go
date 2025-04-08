package kinesisfake

import (
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

type GetRecordsRequest struct {
	Limit         int
	ShardIterator string
	StreamARN     string
}

type GetRecordsResponse struct {
	ChildShards        []Shard
	MillisBehindLatest int
	NextShardIterator  string
	Records            []Record
}

type Record struct {
	ApproximateArrivalTimestamp int
	Data                        []byte
	EncryptionType              string
	PartitionKey                string
	SequenceNumber              string
}

func (f *Fake) getRecords(body []byte) (*GetRecordsResponse, error) {
	var request GetRecordsRequest
	err := json.Unmarshal(body, &request)
	if err != nil {
		return nil, fmt.Errorf("decode GetRecords: %w", err)
	}

	stream := f.db.streams[streamNameFromARN(request.StreamARN)]
	if stream == nil {
		return nil, &ResourceNotFoundException{fmt.Sprintf("no stream %s", request.StreamARN)}
	}

	// Parse the iterator
	shardID, timestamp, pos, err := splitShardIterator(request.ShardIterator)
	if err != nil {
		return nil, fmt.Errorf("invalid shard iterator: %w", err)
	}

	// Check if the iterator has expired
	if timestamp <= f.iteratorsExpirationAt {
		return nil, &ExpiredIteratorException{
			message: fmt.Sprintf("Iterator %s has expired", request.ShardIterator),
		}
	}

	// Find the shard
	shardIndex := slices.IndexFunc(stream.shards, func(s *shard) bool {
		return s.id == shardID
	})
	if shardIndex == -1 {
		return nil, &ResourceNotFoundException{fmt.Sprintf("no shard %s", request.ShardIterator)}
	}
	shard := stream.shards[shardIndex]

	// Get the records from the position
	records := shard.records[pos:]
	responseRecords := make([]Record, len(records))
	for i, r := range records {
		responseRecords[i] = Record{
			ApproximateArrivalTimestamp: 0,
			Data:                        []byte(r.Data),
			PartitionKey:                r.PartitionKey,
			SequenceNumber:              strconv.Itoa(pos + i),
		}
	}

	// Calculate the next position and create the next iterator
	nextPos := pos + len(records)

	// Increment timestamp for the next iterator
	f.lastIteratorTimestamp++
	nextTimestamp := f.lastIteratorTimestamp

	return &GetRecordsResponse{
		NextShardIterator: shardIteratorFor(shardID, nextTimestamp, nextPos),
		Records:           responseRecords,
	}, nil
}

func splitShardIterator(iter string) (string, int, int, error) {
	parts := strings.Split(iter, ":")
	if len(parts) != 3 {
		return "", 0, 0, fmt.Errorf("invalid iterator format: %s", iter)
	}

	timestamp, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return "", 0, 0, fmt.Errorf("invalid timestamp in iterator: %w", err)
	}

	pos, err := strconv.Atoi(parts[2])
	if err != nil {
		return "", 0, 0, fmt.Errorf("invalid position in iterator: %w", err)
	}

	return parts[0], int(timestamp), pos, nil
}
