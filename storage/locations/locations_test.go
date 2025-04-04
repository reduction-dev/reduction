package locations_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"reduction.dev/reduction/storage/localfs"
	"reduction.dev/reduction/storage/locations"
	"reduction.dev/reduction/storage/s3fs"
)

func TestNewLocation_S3Path(t *testing.T) {
	store, err := locations.New("s3://my-bucket/some/path")
	assert.NoError(t, err, "creating store with S3 path should not error")

	// Verify we got an S3 location back
	_, ok := store.(*s3fs.S3Location)
	assert.True(t, ok, "store should be an S3Location for s3:// paths")
}

func TestNewLocation_LocalPath(t *testing.T) {
	store, err := locations.New("/local/path")
	assert.NoError(t, err, "creating store with local path should not error")

	// Verify we got a local directory back
	_, ok := store.(*localfs.Directory)
	assert.True(t, ok, "store should be a Directory for local paths")
}
