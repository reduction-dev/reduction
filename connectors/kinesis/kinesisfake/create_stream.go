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
	max := new(big.Int)
	max.Exp(big.NewInt(2), big.NewInt(128), nil)
	shardRange := new(big.Int).Div(max, big.NewInt(count))

	for i := range int(count) {
		bigIndex := big.NewInt(int64(i))
		start := new(big.Int).Mul(shardRange, bigIndex)
		var end *big.Int
		if i == int(count)-1 {
			end = max
		} else {
			end = new(big.Int).Mul(shardRange, new(big.Int).Sub(bigIndex, big.NewInt(1)))
		}
		shards[i] = &shard{
			id: fmt.Sprintf("shardId-%012d", i),
			hashKeyRange: hashKeyRange{
				startingHashKey: start,
				endingHashKey:   end,
			},
		}
	}

	return shards
}
