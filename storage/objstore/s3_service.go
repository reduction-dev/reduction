package objstore

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3Service interface {
	CopyObject(ctx context.Context, input *s3.CopyObjectInput, options ...func(*s3.Options)) (*s3.CopyObjectOutput, error)
	GetObject(ctx context.Context, input *s3.GetObjectInput, options ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	HeadObject(ctx context.Context, input *s3.HeadObjectInput, options ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	ListObjectsV2(ctx context.Context, input *s3.ListObjectsV2Input, options ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	PutObject(ctx context.Context, input *s3.PutObjectInput, options ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	DeleteObject(ctx context.Context, input *s3.DeleteObjectInput, options ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
}

type S3StorageWithUsage struct {
	S3Service
	usage *S3Usage
}

func NewS3StorageWithUsage(service S3Service) *S3StorageWithUsage {
	return &S3StorageWithUsage{
		S3Service: service,
		usage:     &S3Usage{},
	}
}

func (s *S3StorageWithUsage) CopyObject(ctx context.Context, input *s3.CopyObjectInput, options ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	s.usage.AddExpensiveRequest()
	return s.S3Service.CopyObject(ctx, input, options...)
}

func (s *S3StorageWithUsage) GetObject(ctx context.Context, input *s3.GetObjectInput, options ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	s.usage.AddCheapRequest()
	return s.S3Service.GetObject(ctx, input, options...)
}

func (s *S3StorageWithUsage) HeadObject(ctx context.Context, input *s3.HeadObjectInput, options ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	s.usage.AddCheapRequest()
	return s.S3Service.HeadObject(ctx, input, options...)
}

func (s *S3StorageWithUsage) ListObjectsV2(ctx context.Context, input *s3.ListObjectsV2Input, options ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	s.usage.AddExpensiveRequest()
	return s.S3Service.ListObjectsV2(ctx, input, options...)
}

func (s *S3StorageWithUsage) PutObject(ctx context.Context, input *s3.PutObjectInput, options ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	s.usage.AddExpensiveRequest()
	return s.S3Service.PutObject(ctx, input, options...)
}

func (s *S3StorageWithUsage) DeleteObject(ctx context.Context, input *s3.DeleteObjectInput, options ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	// Delete requests are free
	return s.S3Service.DeleteObject(ctx, input, options...)
}

// TotalCost returns the total cost of all S3 operations performed through this wrapper
func (s *S3StorageWithUsage) TotalCost() string {
	return s.usage.TotalCost()
}
