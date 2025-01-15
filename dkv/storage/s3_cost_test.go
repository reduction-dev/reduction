package storage_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"reduction.dev/reduction/dkv/storage"
)

func TestAddingRequests(t *testing.T) {
	usage := storage.S3Usage{}
	for i := 0; i < 1_000; i++ {
		usage.AddCheapRequest()
	}

	assert.Equal(t, "$0.0004", usage.TotalCost())

	usage = storage.S3Usage{}
	for i := 0; i < 1_000_000; i++ {
		usage.AddCheapRequest()
	}
	assert.Equal(t, "$0.40", usage.TotalCost())

	usage = storage.S3Usage{}
	usage.AddCheapRequest()
	assert.Equal(t, "$0.0000", usage.TotalCost())
}
