package kinesisfake

import (
	"encoding/json"
	"fmt"
	"slices"
	"strconv"

	kinesistypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"reduction.dev/reduction/util/ptr"
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

	if request.ShardId == "" {
		return nil, &kinesistypes.ResourceNotFoundException{
			Message: ptr.New("ShardId is required"),
		}
	}

	stream := f.db.streams[streamNameFromARN(request.StreamARN)]
	if stream == nil {
		return nil, &kinesistypes.ResourceNotFoundException{
			Message: ptr.New(fmt.Sprintf("Stream %s not found", request.StreamARN)),
		}
	}

	hasShard := slices.ContainsFunc(stream.shards, func(s *shard) bool {
		return s.id == request.ShardId
	})
	if !hasShard {
		return nil, &kinesistypes.ResourceNotFoundException{
			Message: ptr.New(fmt.Sprintf("Shard %s not found", request.ShardId)),
		}
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

	// Increment the timestamp for this iterator using atomic operations
	timestamp := f.lastIteratorTimestamp.Add(1)
	shardIterator := shardIteratorFor(request.ShardId, timestamp, position)

	return &GetShardIteratorResponse{ShardIterator: shardIterator}, nil
}

func shardIteratorFor(shardID string, timestamp int64, position int) string {
	return shardID + ":" + strconv.FormatInt(timestamp, 10) + ":" + strconv.Itoa(position)
}
