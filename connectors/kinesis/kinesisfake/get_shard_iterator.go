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

func (f *Fake) GetShardIterator(body []byte) (*GetShardIteratorResponse, error) {
	var request GetShardIteratorRequest
	err := json.Unmarshal(body, &request)
	if err != nil {
		return nil, fmt.Errorf("decode ListShardsRequest: %w", err)
	}

	stream := f.db.streams[streamNameFromARN(request.StreamARN)]
	if stream == nil {
		return nil, &ResourceNotFoundException{}
	}
	shardIterator := shardIteratorFor(request.ShardId, 0)

	return &GetShardIteratorResponse{ShardIterator: shardIterator}, nil
}

func shardIteratorFor(shardID string, position int) string {
	return shardID + ":" + strconv.Itoa(position)
}
