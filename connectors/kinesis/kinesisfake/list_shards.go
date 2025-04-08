package kinesisfake

import (
	"encoding/json"
	"fmt"
)

type ListShardsRequest struct {
	ExclusiveStartShardId   string
	MaxResults              int64
	NextToken               string
	ShardFilter             *ShardFilter
	StreamARN               string
	StreamCreationTimestamp int64
	StreamName              string
}

type ShardFilter struct {
	ShardId   string
	Timestamp int64
	Type      string
}

type ListShardsResponse struct {
	NextToken *string
	Shards    []Shard
}

func (f *Fake) listShards(body []byte) (*ListShardsResponse, error) {
	var request PutRecordsRequest
	err := json.Unmarshal(body, &request)
	if err != nil {
		return nil, fmt.Errorf("decode ListShardsRequest: %w", err)
	}

	stream := f.db.streams[streamNameFromARN(request.StreamARN)]
	responseShards := make([]Shard, len(stream.shards))
	for i, shardRecord := range stream.shards {
		responseShards[i] = Shard{ShardId: shardRecord.id}
	}

	return &ListShardsResponse{Shards: responseShards}, nil
}
