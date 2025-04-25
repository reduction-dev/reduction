package kinesisfake

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"time"

	kinesistypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"reduction.dev/reduction/util/ptr"
)

type PutRecordsRequest struct {
	Records   []*PutRecordsRequestEntry
	StreamARN string
}

type PutRecordsRequestEntry struct {
	Data         string
	PartitionKey string
}

type PutRecordsResponseEntry struct {
	ErrorCode      *string
	ErrorMessage   *string
	ShardId        string
	SequenceNumber string
}

type PutRecordsResponse struct {
	Records    []*PutRecordsResponseEntry
	StreamName string
}

func (f *Fake) putRecords(body []byte) (*PutRecordsResponse, error) {
	var request PutRecordsRequest
	err := json.Unmarshal(body, &request)
	if err != nil {
		return nil, fmt.Errorf("decode PutRecordsRequest: %w", err)
	}

	streamName := streamNameFromARN(request.StreamARN)
	stream := f.db.streams[streamName]
	if stream == nil {
		return nil, &kinesistypes.ResourceNotFoundException{
			Message: ptr.New(fmt.Sprintf("Stream %s under account %s not found.", streamName, "123456789012")),
		}
	}

	// Only pick from consider open shards
	openShards := make([]*shard, 0, len(stream.shards))
	for _, s := range stream.shards {
		if !s.isFinished {
			openShards = append(openShards, s)
		}
	}
	if len(openShards) == 0 {
		return nil, fmt.Errorf("no open shards available for writing")
	}

	for i, r := range request.Records {
		data, err := base64.StdEncoding.DecodeString(r.Data)
		if err != nil {
			panic(err)
		}

		shard := pickShard(r.PartitionKey, openShards)
		shard.records = append(shard.records, Record{
			ApproximateArrivalTimestamp: float64(time.Now().UnixNano()) / 1e9,
			Data:                        data,
			PartitionKey:                r.PartitionKey,
			SequenceNumber:              strconv.Itoa(len(shard.records) + i),
		})
	}

	return &PutRecordsResponse{}, nil
}

func pickShard(partitionKey string, shards []*shard) *shard {
	hash := md5.Sum([]byte(partitionKey))
	rangeKey := big.NewInt(0).SetBytes(hash[:16])

	for _, s := range shards {
		if s.hashKeyRange.includes(rangeKey) {
			return s
		}
	}

	return shards[0]
}
