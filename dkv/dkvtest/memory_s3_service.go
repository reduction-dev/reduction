package dkvtest

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"path"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"reduction.dev/reduction/dkv/storage"
)

// MemoryS3Service is an in-memory implementation of the S3Service for testing.
type MemoryS3Service struct {
	data map[string][]byte
}

func NewMemoryS3Service() MemoryS3Service {
	return MemoryS3Service{
		data: make(map[string][]byte),
	}
}

func (m *MemoryS3Service) CopyObject(ctx context.Context, input *s3.CopyObjectInput) (*s3.CopyObjectOutput, error) {
	sourceData, ok := m.data[*input.CopySource]
	if !ok {
		slog.Error("source key not found", "key", *input.CopySource)
		return nil, &types.NoSuchKey{}
	}

	newData := make([]byte, len(sourceData))
	copy(newData, sourceData)
	m.data[path.Join(*input.Bucket, *input.Key)] = newData
	return &s3.CopyObjectOutput{}, nil
}

func (m *MemoryS3Service) GetObject(ctx context.Context, input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	data, ok := m.data[path.Join(*input.Bucket, *input.Key)]
	if !ok {
		return nil, &types.NoSuchKey{}
	}

	return &s3.GetObjectOutput{
		Body: io.NopCloser(bytes.NewReader(data)),
	}, nil
}

func (m *MemoryS3Service) PutObject(ctx context.Context, input *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	buf, err := io.ReadAll(input.Body)
	if err != nil {
		return nil, err
	}

	m.data[path.Join(*input.Bucket, *input.Key)] = buf
	return &s3.PutObjectOutput{}, nil
}

func (m *MemoryS3Service) DeleteObject(ctx context.Context, input *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	delete(m.data, path.Join(*input.Bucket, *input.Key))
	return &s3.DeleteObjectOutput{}, nil
}

var _ storage.S3Service = (*MemoryS3Service)(nil)
