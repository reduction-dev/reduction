package kinesisfake

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
)

type DescribeStreamRequest struct {
	StreamName string
}

type DescribeStreamResponse struct {
	StreamDescription StreamDescription
}
type ShardLevelMetric struct {
	MetricName string
}

type EnhancedMonitoring struct {
	ShardLevelMetrics []ShardLevelMetric
}

type HashKeyRange struct {
	StartingHashKey string
	EndingHashKey   string
}

type SequenceNumberRange struct {
	StartingSequenceNumber string
	EndingSequenceNumber   string
}

type Shard struct {
	AdjacentParentShardId string
	HashKeyRange          HashKeyRange
	ParentShardId         string
	SequenceNumberRange   SequenceNumberRange
	ShardId               string
}

type StreamModeDetails struct {
	StreamMode string
}

type StreamDescription struct {
	EncryptionType          string
	EnhancedMonitoring      []EnhancedMonitoring
	HasMoreShards           bool
	KeyId                   string
	RetentionPeriodHours    int
	Shards                  []Shard
	StreamARN               string
	StreamCreationTimestamp int64
	StreamModeDetails       StreamModeDetails
	StreamName              string
	StreamStatus            string
}

func (f *Fake) describeStream(body []byte) (*DescribeStreamResponse, error) {
	var request DescribeStreamRequest
	err := json.Unmarshal(body, &request)
	if err != nil {
		return nil, err
	}

	stream := f.db.streams[request.StreamName]
	slog.Info("streams", "s", stream)
	if stream == nil {
		slog.Info("sending ResourceNotFoundException")
		return nil, &ResourceNotFoundException{}
	}
	return &DescribeStreamResponse{
		StreamDescription: StreamDescription{
			StreamName:   request.StreamName,
			StreamStatus: "ACTIVE",
			StreamARN:    arnFromStreamName(request.StreamName),
		},
	}, nil
}

func arnFromStreamName(streamName string) string {
	return fmt.Sprintf("arn:aws:kinesis:region:123456789012:stream/%s", streamName)
}

func streamNameFromARN(arn string) string {
	parts := strings.Split(arn, "/")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}
	return ""
}
