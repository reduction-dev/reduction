package kinesisfake

import (
	"encoding/json"
	"fmt"
	"math/big"

	kinesistypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"reduction.dev/reduction/util/ptr"
)

type MergeShardsRequest struct {
	StreamARN            string
	StreamName           string // allow but error if set
	ShardToMerge         string
	AdjacentShardToMerge string
}

type MergeShardsResponse struct{}

func (f *Fake) mergeShards(body []byte) (*MergeShardsResponse, error) {
	var req MergeShardsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("decode MergeShardsRequest: %w", err)
	}
	if req.StreamName != "" {
		return nil, fmt.Errorf("MergeShards: StreamName is not supported, use StreamARN")
	}
	if req.StreamARN == "" {
		return nil, fmt.Errorf("MergeShards: StreamARN must be set")
	}
	streamName := streamNameFromARN(req.StreamARN)
	stream, ok := f.db.streams[streamName]
	if !ok {
		return nil, &kinesistypes.ResourceNotFoundException{
			Message: ptr.New(fmt.Sprintf("Stream %s not found", req.StreamARN)),
		}
	}

	var shardA, shardB *shard
	for _, s := range stream.shards {
		if s.id == req.ShardToMerge {
			shardA = s
		}
		if s.id == req.AdjacentShardToMerge {
			shardB = s
		}
	}
	if shardA == nil || shardB == nil {
		return nil, &kinesistypes.ResourceNotFoundException{
			Message: ptr.New("Shard(s) not found for merge"),
		}
	}

	// Mark both as finished
	shardA.isFinished = true
	shardB.isFinished = true

	// Create new shard with combined hash key range
	newShardID := fmt.Sprintf("shardId-%012d", len(stream.shards))
	newShard := &shard{
		id: newShardID,
		hashKeyRange: hashKeyRange{
			startingHashKey: minBigInt(shardA.hashKeyRange.startingHashKey, shardB.hashKeyRange.startingHashKey),
			endingHashKey:   maxBigInt(shardA.hashKeyRange.endingHashKey, shardB.hashKeyRange.endingHashKey),
		},
		parentShard:         shardA.id,
		adjacentParentShard: shardB.id,
	}
	stream.shards = append(stream.shards, newShard)

	return &MergeShardsResponse{}, nil
}

func minBigInt(a, b *big.Int) *big.Int {
	if a.Cmp(b) <= 0 {
		return a
	}
	return b
}

func maxBigInt(a, b *big.Int) *big.Int {
	if a.Cmp(b) >= 0 {
		return a
	}
	return b
}
