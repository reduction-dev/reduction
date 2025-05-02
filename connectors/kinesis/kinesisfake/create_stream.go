package kinesisfake

import (
	"encoding/json"
	"fmt"
	"math/big"
)

type CreateStreamRequest struct {
	ShardCount int64
	StreamName string
}

type CreateStreamResponse struct{}

func (f *Fake) createStream(body []byte) (*CreateStreamResponse, error) {
	var request CreateStreamRequest
	err := json.Unmarshal(body, &request)
	if err != nil {
		return nil, err
	}

	f.db.streams[request.StreamName] = &stream{
		shards: createShards(request.ShardCount),
	}

	return &CreateStreamResponse{}, nil
}

func createShards(count int64) []*shard {
	shards := make([]*shard, count)
	max := new(big.Int).Exp(big.NewInt(2), big.NewInt(128), nil)
	shardRange := new(big.Int).Div(max, big.NewInt(count))

	start := big.NewInt(0)
	for i := int64(0); i < count; i++ {
		end := new(big.Int).Add(start, shardRange)
		end.Sub(end, big.NewInt(1))
		if i == count-1 {
			end.Set(max)
			end.Sub(end, big.NewInt(1)) // last shard ends at 2^128-1
		}
		shards[i] = &shard{
			id: fmt.Sprintf("shardId-%012d", i),
			hashKeyRange: hashKeyRange{
				startingHashKey: new(big.Int).Set(start),
				endingHashKey:   new(big.Int).Set(end),
			},
		}
		start = new(big.Int).Add(end, big.NewInt(1))
	}
	return shards
}
