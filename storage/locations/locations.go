package locations

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"reduction.dev/reduction/storage"
	"reduction.dev/reduction/storage/localfs"
	"reduction.dev/reduction/storage/s3fs"
)

// New creates a FileStore type from the given path. Returns an
// s3fs.S3Location if the path is an S3 URI, otherwise returns a local file
// system location.
func New(path string) (storage.FileStore, error) {
	// Check if the path is an S3 URI
	if strings.HasPrefix(path, "s3://") {
		cfg, err := config.LoadDefaultConfig(context.Background())
		if err != nil {
			return nil, fmt.Errorf("failed to load AWS configuration: %w", err)
		}

		client := s3.NewFromConfig(cfg)
		return s3fs.NewS3Location(client, path), nil
	}

	// Otherwise, return a local file system location
	return localfs.NewDirectory(path), nil
}
