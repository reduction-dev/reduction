// Package fileu provides utilities for file operations.
package fileu

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// ReadFile reads the contents of the file specified by path.  It supports both
// local file paths and S3 URLs like s3://bucket/path/object.json.
func ReadFile(path string) ([]byte, error) {
	if strings.HasPrefix(path, "s3://") {
		return readS3File(path)
	}
	return os.ReadFile(path)
}

// readS3File reads a file from S3 given an s3:// URL.
func readS3File(s3URL string) ([]byte, error) {
	// Parse the S3 URL to extract bucket and key
	parsedURL, err := url.Parse(s3URL)
	if err != nil {
		return nil, fmt.Errorf("invalid S3 URL: %w", err)
	}

	// The bucket is the host part of the URL
	bucket := parsedURL.Host

	// The key is the path without the leading slash
	key := strings.TrimPrefix(parsedURL.Path, "/")

	// Create an AWS SDK config
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS configuration: %w", err)
	}

	// Create an S3 client
	s3Client := s3.NewFromConfig(cfg)

	// Get the object from S3
	result, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get object from S3: %w", err)
	}
	defer result.Body.Close()

	// Read the content from S3
	data, err := io.ReadAll(result.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read object content from S3: %w", err)
	}

	return data, nil
}
