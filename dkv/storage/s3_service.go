package storage

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3Service interface {
	CopyObject(ctx context.Context, input *s3.CopyObjectInput) (*s3.CopyObjectOutput, error)
	GetObject(ctx context.Context, input *s3.GetObjectInput) (*s3.GetObjectOutput, error)
	PutObject(ctx context.Context, input *s3.PutObjectInput) (*s3.PutObjectOutput, error)
	DeleteObject(ctx context.Context, input *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error)
}

// AWSS3Service is the concrete implementation of AWS S3.
type AWSS3Service struct {
	client *s3.Client
}

func NewAWSS3Service(client *s3.Client) AWSS3Service {
	return AWSS3Service{client: client}
}

func (a *AWSS3Service) CopyObject(ctx context.Context, input *s3.CopyObjectInput) (*s3.CopyObjectOutput, error) {
	return a.client.CopyObject(ctx, input)
}

func (a *AWSS3Service) GetObject(ctx context.Context, input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	return a.client.GetObject(ctx, input)
}

func (a *AWSS3Service) PutObject(ctx context.Context, input *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	return a.client.PutObject(ctx, input)
}

func (a *AWSS3Service) DeleteObject(ctx context.Context, input *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	return a.client.DeleteObject(ctx, input)
}

var _ S3Service = (*AWSS3Service)(nil)
