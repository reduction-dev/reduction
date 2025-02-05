package kinesisfake

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"time"
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

func (f *Fake) PutRecords(body []byte) (*PutRecordsResponse, error) {
	var request PutRecordsRequest
	err := json.Unmarshal(body, &request)
	if err != nil {
		return nil, fmt.Errorf("decode PutRecordsRequest: %w", err)
	}

	stream := f.db.streams[streamNameFromARN(request.StreamARN)]

	for i, r := range request.Records {
		data, err := base64.StdEncoding.DecodeString(r.Data)
		if err != nil {
			panic(err)
		}

		shard := pickShard(r.PartitionKey, stream.shards)
		shard.records = append(shard.records, Record{
			ApproximateArrivalTimestamp: int(time.Now().UnixMilli()),
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
