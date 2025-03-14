package kinesis

import (
	"context"
	"fmt"
	"log"
	"time"

	"google.golang.org/protobuf/proto"
	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/connectors/kinesis/kinesispb"
	"reduction.dev/reduction/proto/workerpb"
	"reduction.dev/reduction/util/sliceu"

	"github.com/aws/aws-sdk-go-v2/aws"
)

type SourceSplitter struct {
	client    *Client
	streamARN string
	shardIDs  []string
	cursors   map[string]string
}

func NewSourceSplitter(params SourceConfig) *SourceSplitter {
	client := params.Client
	if client == nil {
		var err error
		client, err = NewClient(&NewClientParams{
			Endpoint:    params.Endpoint,
			Region:      "us-east-2",
			Credentials: aws.AnonymousCredentials{}})
		if err != nil {
			log.Fatalf("Failed to create Kinesis Client: %s", err)
		}
	}

	return &SourceSplitter{
		client:    client,
		streamARN: params.StreamARN,
		cursors:   make(map[string]string),
	}
}

func (s *SourceSplitter) AssignSplits(ids []string) (map[string][]*workerpb.SourceSplit, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var err error
	s.shardIDs, err = s.client.ListShards(ctx, s.streamARN)
	if err != nil {
		return nil, fmt.Errorf("kinesis.SourceSplitter failed to discover splits: %w", err)
	}

	assignments := make(map[string][]*workerpb.SourceSplit, len(ids))
	shardIDGroups := sliceu.Partition(s.shardIDs, len(ids))
	for i, id := range ids {
		assignedShards := shardIDGroups[i]
		assignments[id] = make([]*workerpb.SourceSplit, len(assignedShards))
		for i, shardID := range assignedShards {
			assignments[id][i] = &workerpb.SourceSplit{
				SourceId: "tbd",
				SplitId:  shardID,
				Cursor:   []byte(s.cursors[shardID]),
			}
		}
	}

	return assignments, nil
}

// LoadCheckpoints receives checkpoint data created by source readers
// and loads cursor data for each shard id.
func (s *SourceSplitter) LoadCheckpoints(checkpoints [][]byte) error {
	for _, cpData := range checkpoints {
		var checkpoint kinesispb.Checkpoint
		if err := proto.Unmarshal(cpData, &checkpoint); err != nil {
			return err
		}
		for _, shard := range checkpoint.Shards {
			s.cursors[shard.ShardId] = shard.Cursor
		}
	}
	return nil
}

func (s *SourceSplitter) IsSourceSplitter() {}

var _ connectors.SourceSplitter = (*SourceSplitter)(nil)
