package objstore

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3Service interface {
	CopyObject(ctx context.Context, input *s3.CopyObjectInput) (*s3.CopyObjectOutput, error)
	GetObject(ctx context.Context, input *s3.GetObjectInput) (*s3.GetObjectOutput, error)
	ListObjectsV2(ctx context.Context, input *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error)
	PutObject(ctx context.Context, input *s3.PutObjectInput) (*s3.PutObjectOutput, error)
	DeleteObject(ctx context.Context, input *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error)
}
