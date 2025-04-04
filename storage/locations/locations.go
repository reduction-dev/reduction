package locations

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// New creates a StorageLocation type from the given path. Returns an
// S3Location if the path is an S3 URI, otherwise returns a local file
// system location.
func New(path string) (StorageLocation, error) {
	// Check if the path is an S3 URI
	if strings.HasPrefix(path, "s3://") {
		cfg, err := config.LoadDefaultConfig(context.Background())
		if err != nil {
			return nil, fmt.Errorf("failed to load AWS configuration: %w", err)
		}

		client := s3.NewFromConfig(cfg)
		return NewS3Location(client, path), nil
	}

	// Otherwise, return a local file system location
	return NewLocal(path), nil
}
