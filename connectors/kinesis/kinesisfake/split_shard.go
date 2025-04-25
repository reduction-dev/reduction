package kinesisfake

import (
	"encoding/json"
	"fmt"
	"math/big"
)

type SplitShardRequest struct {
	StreamName         string
	StreamARN          string
	ShardToSplit       string
	NewStartingHashKey string
}

type SplitShardResponse struct{}

func (f *Fake) splitShard(body []byte) (*SplitShardResponse, error) {
	var req SplitShardRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("decode SplitShardRequest: %w", err)
	}

	streamName := req.StreamName
	if streamName == "" && req.StreamARN != "" {
		streamName = streamNameFromARN(req.StreamARN)
	}
	stream := f.db.streams[streamName]
	if stream == nil {
		return nil, fmt.Errorf("stream %s not found", streamName)
	}

	var shardIdx int = -1
	for i, s := range stream.shards {
		if s.id == req.ShardToSplit {
			shardIdx = i
			break
		}
	}
	if shardIdx == -1 {
		return nil, fmt.Errorf("shard %s not found", req.ShardToSplit)
	}

	targetShard := stream.shards[shardIdx]
	if targetShard.isFinished {
		return nil, fmt.Errorf("cannot split closed shard %s", targetShard.id)
	}

	newStart := new(big.Int)
	if _, ok := newStart.SetString(req.NewStartingHashKey, 10); !ok {
		return nil, fmt.Errorf("invalid NewStartingHashKey: %s", req.NewStartingHashKey)
	}

	if newStart.Cmp(targetShard.hashKeyRange.startingHashKey) <= 0 || newStart.Cmp(targetShard.hashKeyRange.endingHashKey) >= 0 {
		return nil, fmt.Errorf("NewStartingHashKey must be within the range of the shard's hash key range")
	}

	// Close the original shard
	targetShard.isFinished = true

	// Create two new shards
	left := &shard{
		id: fmt.Sprintf("shardId-%012d", len(stream.shards)),
		hashKeyRange: hashKeyRange{
			startingHashKey: targetShard.hashKeyRange.startingHashKey,
			endingHashKey:   new(big.Int).Sub(newStart, big.NewInt(1)),
		},
		parentShard: targetShard.id,
	}
	right := &shard{
		id: fmt.Sprintf("shardId-%012d", len(stream.shards)+1),
		hashKeyRange: hashKeyRange{
			startingHashKey: newStart,
			endingHashKey:   targetShard.hashKeyRange.endingHashKey,
		},
		parentShard: targetShard.id,
	}
	// Insert the new shards
	stream.shards = append(stream.shards, left, right)

	return &SplitShardResponse{}, nil
}
