package locations

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"reduction.dev/reduction/storage/objstore"
)

func ReadFile(path string) ([]byte, error) {
	if strings.HasPrefix(path, "s3://") {
		cfg, err := config.LoadDefaultConfig(context.Background())
		if err != nil {
			return nil, fmt.Errorf("failed to load AWS configuration: %w", err)
		}

		return ReadS3File(s3.NewFromConfig(cfg), path)
	}
	return ReadLocalFile(path)
}

func ReadLocalFile(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("opening file %s: %w", path, err)
	}
	defer file.Close()

	return io.ReadAll(file)
}

func ReadS3File(s3Client objstore.S3Service, path string) ([]byte, error) {
	// Remove s3:// prefix if present
	path = strings.TrimPrefix(path, "s3://")

	// Split into bucket and key
	parts := strings.SplitN(path, "/", 2)
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid S3 path, must include bucket and key: %s", path)
	}

	bucket, key := parts[0], parts[1]
	output, err := s3Client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		var noSuchKey *types.NoSuchKey
		if errors.As(err, &noSuchKey) {
			return nil, fmt.Errorf("file not found at %s: %w", path, ErrNotFound)
		}
		return nil, fmt.Errorf("failed to read object: %w", err)
	}

	defer output.Body.Close()
	return io.ReadAll(output.Body)
}
