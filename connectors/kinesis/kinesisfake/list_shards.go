package kinesisfake

import (
	"encoding/json"
	"fmt"
	"slices"
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
	var request ListShardsRequest
	if err := json.Unmarshal(body, &request); err != nil {
		return nil, fmt.Errorf("decode ListShardsRequest: %w", err)
	}

	streamName := request.StreamName
	if streamName == "" && request.StreamARN != "" {
		streamName = streamNameFromARN(request.StreamARN)
	}
	stream := f.db.streams[streamName]
	if stream == nil {
		return nil, fmt.Errorf("stream %s not found", streamName)
	}

	// Find the starting index based on ExclusiveStartShardId or NextToken
	startIdx := 0
	if request.ExclusiveStartShardId != "" {
		foundShardID := slices.IndexFunc(stream.shards, func(s *shard) bool {
			return s.id == request.ExclusiveStartShardId
		})
		if foundShardID == -1 {
			return nil, fmt.Errorf("exclusive start shard %s not found", request.ExclusiveStartShardId)
		} else {
			startIdx = foundShardID + 1
		}
	} else if request.NextToken != "" {
		// NextToken is just the stringified start index
		fmt.Sscanf(request.NextToken, "%d", &startIdx)
	}

	// Determine how many shards to return
	maxResults := int(request.MaxResults)
	if maxResults <= 0 || maxResults > len(stream.shards)-startIdx {
		maxResults = len(stream.shards) - startIdx
	}

	endIdx := min(startIdx+maxResults, len(stream.shards))

	responseShards := make([]Shard, endIdx-startIdx)
	for i, shardRecord := range stream.shards[startIdx:endIdx] {
		responseShards[i] = Shard{
			ShardId: shardRecord.id,
			HashKeyRange: HashKeyRange{
				StartingHashKey: shardRecord.hashKeyRange.startingHashKey.String(),
				EndingHashKey:   shardRecord.hashKeyRange.endingHashKey.String(),
			},
			SequenceNumberRange: SequenceNumberRange{
				StartingSequenceNumber: "0",
				EndingSequenceNumber:   "",
			},
			ParentShardId:         "",
			AdjacentParentShardId: "",
		}
	}

	var nextToken *string
	if endIdx < len(stream.shards) {
		tok := fmt.Sprintf("%d", endIdx)
		nextToken = &tok
	}

	return &ListShardsResponse{
		Shards:    responseShards,
		NextToken: nextToken,
	}, nil
}
