package kinesis

import (
	"context"
	"fmt"
	"time"

	"reduction.dev/reduction/util/ptr"
	"reduction.dev/reduction/util/sliceu"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

type Client struct {
	svc *kinesis.Client
}

type NewClientParams struct {
	// The kinesis endpoint to use. Normally left blank but used for testing
	// against fake.
	Endpoint string
	Region   string
	// The AWS credentials profile name to use instead of default when credentials
	// falls back to credentials config file.
	Profile     string
	Credentials aws.CredentialsProvider
}

func NewClient(params *NewClientParams) (*Client, error) {
	cfg, err := config.LoadDefaultConfig(context.Background(),
		func(lo *config.LoadOptions) error {
			if params.Region != "" {
				lo.Region = params.Region
			}
			if params.Profile != "" {
				lo.SharedConfigProfile = params.Profile
			}
			if params.Credentials != nil {
				lo.Credentials = params.Credentials
			}

			return nil
		},
	)
	if err != nil {
		return nil, fmt.Errorf("kinesis load config: %w", err)
	}

	svc := kinesis.NewFromConfig(cfg, func(opts *kinesis.Options) {
		if params.Endpoint != "" {
			opts.BaseEndpoint = ptr.New(params.Endpoint)
		}
	})

	return &Client{svc: svc}, nil
}

type Record struct {
	Key  string
	Data []byte
}

func (c *Client) PutRecordBatch(ctx context.Context, streamARN string, records []Record) error {
	entries := make([]types.PutRecordsRequestEntry, len(records))
	for i, e := range records {
		entries[i] = types.PutRecordsRequestEntry{Data: e.Data, PartitionKey: &e.Key}
	}
	recordBatch := &kinesis.PutRecordsInput{
		Records:   entries,
		StreamARN: ptr.New(streamARN),
	}

	if _, err := c.svc.PutRecords(ctx, recordBatch); err != nil {
		return fmt.Errorf("put record: %w", err)
	}

	return nil
}

type CreateStreamParams struct {
	StreamName      string
	ShardCount      int
	MaxWaitDuration time.Duration
}

func (c *Client) CreateStream(ctx context.Context, params *CreateStreamParams) (string, error) {
	if _, err := c.svc.CreateStream(ctx, &kinesis.CreateStreamInput{
		StreamName: &params.StreamName,
		ShardCount: ptr.New(int32(params.ShardCount)),
	}); err != nil {
		return "", fmt.Errorf("create stream: %w", err)
	}

	describeInput := &kinesis.DescribeStreamInput{StreamName: &params.StreamName}
	w := kinesis.NewStreamExistsWaiter(c.svc)
	if err := w.Wait(ctx, describeInput, params.MaxWaitDuration); err != nil {
		return "", fmt.Errorf("create stream waiting: %w", err)
	}

	out, err := c.svc.DescribeStream(ctx, describeInput)
	if err != nil {
		return "", fmt.Errorf("create stream describe stream: %w", err)
	}

	if out.StreamDescription.StreamARN == nil {
		return "", fmt.Errorf("invalid stream description output: %+v", out.StreamDescription)
	}

	return *out.StreamDescription.StreamARN, nil
}

func (c *Client) ListShards(ctx context.Context, streamARN string) ([]string, error) {
	var shardIDs []string

	var lastSeenShardID *string
	for {
		input := &kinesis.ListShardsInput{
			ExclusiveStartShardId: lastSeenShardID,
			StreamARN:             &streamARN,
		}

		out, err := c.svc.ListShards(ctx, input)
		if err != nil {
			return nil, err
		}

		for _, s := range out.Shards {
			shardIDs = append(shardIDs, *s.ShardId)
		}

		if out.NextToken == nil {
			break
		}
		lastSeenShardID = ptr.New(sliceu.Last(shardIDs))
	}

	return shardIDs, nil
}

type DeleteStreamParams struct {
	StreamName      string
	MaxWaitDuration time.Duration
}

func (c *Client) DeleteStream(ctx context.Context, params *DeleteStreamParams) error {
	input := &kinesis.DeleteStreamInput{StreamName: &params.StreamName}
	if _, err := c.svc.DeleteStream(ctx, input); err != nil {
		return fmt.Errorf("delete stream: %w", err)
	}

	w := kinesis.NewStreamNotExistsWaiter(c.svc)
	if err := w.Wait(
		ctx,
		&kinesis.DescribeStreamInput{StreamName: &params.StreamName},
		30*time.Second,
		func(snewo *kinesis.StreamNotExistsWaiterOptions) {
			snewo.MinDelay = 1 * time.Millisecond
		}); err != nil {
		return fmt.Errorf("delete stream waiting: %w", err)
	}

	return nil
}

func (c *Client) GetRecords(ctx context.Context, params *kinesis.GetRecordsInput) (*kinesis.GetRecordsOutput, error) {
	out, err := c.svc.GetRecords(ctx, params)
	if err != nil {
		return nil, err
	}

	return out, nil
}

func (c *Client) GetShardIterator(ctx context.Context, params *kinesis.GetShardIteratorInput) (string, error) {
	out, err := c.svc.GetShardIterator(ctx, params)
	if err != nil {
		return "", err
	}

	return *out.ShardIterator, nil
}
