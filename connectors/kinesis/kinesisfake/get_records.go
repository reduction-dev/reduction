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

func (f *Fake) GetRecords(body []byte) (*GetRecordsResponse, error) {
	var request GetRecordsRequest
	err := json.Unmarshal(body, &request)
	if err != nil {
		return nil, fmt.Errorf("decode GetRecords: %w", err)
	}

	stream := f.db.streams[streamNameFromARN(request.StreamARN)]
	if stream == nil {
		return nil, &ResourceNotFoundException{fmt.Sprintf("no stream %s", request.StreamARN)}
	}

	shardID, pos := splitShardIterator(request.ShardIterator)
	shardIndex := slices.IndexFunc(stream.shards, func(s *shard) bool {
		return s.id == shardID
	})
	if shardIndex == -1 {
		return nil, &ResourceNotFoundException{fmt.Sprintf("no shard %s", request.ShardIterator)}
	}
	shard := stream.shards[shardIndex]

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

	nextPos := pos + len(records)
	return &GetRecordsResponse{
		NextShardIterator: shardID + ":" + strconv.Itoa(nextPos),
		Records:           responseRecords,
	}, nil
}

func splitShardIterator(iter string) (string, int) {
	parts := strings.Split(iter, ":")
	pos, err := strconv.Atoi(parts[1])
	if err != nil {
		panic(err)
	}
	return parts[0], pos
}
