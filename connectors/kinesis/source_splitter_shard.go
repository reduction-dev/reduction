package kinesis

import (
	"math/big"

	kinesistypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"reduction.dev/reduction/connectors/kinesis/kinesispb"
)

type SourceSplitterShard struct {
	ShardID      string
	HashKeyRange struct {
		Start big.Int
		End   big.Int
	}
	ParentIDs []string
}

func newSourceSplitterShardFromKinesis(shard kinesistypes.Shard) SourceSplitterShard {
	var start, end big.Int
	start.SetString(*shard.HashKeyRange.StartingHashKey, 10)
	end.SetString(*shard.HashKeyRange.EndingHashKey, 10)

	parentIDs := make([]string, 0, 2)
	if shard.ParentShardId != nil {
		parentIDs = append(parentIDs, *shard.ParentShardId)
	}
	if shard.AdjacentParentShardId != nil {
		parentIDs = append(parentIDs, *shard.AdjacentParentShardId)
	}

	return SourceSplitterShard{
		ShardID: *shard.ShardId,
		HashKeyRange: struct {
			Start big.Int
			End   big.Int
		}{
			Start: start,
			End:   end,
		},
		ParentIDs: parentIDs,
	}
}

func newSourceSplitterShardFromProto(shard *kinesispb.SourceSplitterShard) SourceSplitterShard {
	var start, end big.Int
	start.SetBytes(shard.HashKeyRange.Start)
	end.SetBytes(shard.HashKeyRange.End)

	return SourceSplitterShard{
		ShardID: shard.ShardId,
		HashKeyRange: struct {
			Start big.Int
			End   big.Int
		}{
			Start: start,
			End:   end,
		},
		ParentIDs: shard.ParentShardIds,
	}
}

func (s SourceSplitterShard) toProto() *kinesispb.SourceSplitterShard {
	return &kinesispb.SourceSplitterShard{
		ShardId: s.ShardID,
		HashKeyRange: &kinesispb.HashKeyRange{
			Start: s.HashKeyRange.Start.Bytes(),
			End:   s.HashKeyRange.End.Bytes(),
		},
		ParentShardIds: s.ParentIDs,
	}
}
