package kinesisfake

import (
	"encoding/json"
	"fmt"
	"strconv"
)

type GetShardIteratorRequest struct {
	ShardId                string
	ShardIteratorType      string
	StartingSequenceNumber string
	StreamARN              string
	StreamName             string
	Timestamp              int
}

type GetShardIteratorResponse struct {
	ShardIterator string
}

func (f *Fake) getShardIterator(body []byte) (*GetShardIteratorResponse, error) {
	var request GetShardIteratorRequest
	err := json.Unmarshal(body, &request)
	if err != nil {
		return nil, fmt.Errorf("decode ListShardsRequest: %w", err)
	}

	stream := f.db.streams[streamNameFromARN(request.StreamARN)]
	if stream == nil {
		return nil, &ResourceNotFoundException{}
	}

	position := 0

	// Handle different iterator types
	if request.ShardIteratorType == "AFTER_SEQUENCE_NUMBER" && request.StartingSequenceNumber != "" {
		seqNum, err := strconv.Atoi(request.StartingSequenceNumber)
		if err != nil {
			return nil, fmt.Errorf("invalid sequence number: %w", err)
		}
		// For AFTER_SEQUENCE_NUMBER, we want to start at the position after the requested sequence number
		position = seqNum + 1
	}

	shardIterator := shardIteratorFor(request.ShardId, position)
	f.db.activeShardIterators[shardIterator] = true

	return &GetShardIteratorResponse{ShardIterator: shardIterator}, nil
}

func shardIteratorFor(shardID string, position int) string {
	return shardID + ":" + strconv.Itoa(position)
}
