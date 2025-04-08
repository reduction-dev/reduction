package objstore_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"reduction.dev/reduction/storage/objstore"
)

func TestAddingRequests(t *testing.T) {
	usage := objstore.S3Usage{}
	for range 1_000 {
		usage.AddCheapRequest()
	}

	assert.Equal(t, "$0.0004", usage.TotalCost())

	usage = objstore.S3Usage{}
	for range 1_000_000 {
		usage.AddCheapRequest()
	}
	assert.Equal(t, "$0.40", usage.TotalCost())

	usage = objstore.S3Usage{}
	usage.AddCheapRequest()
	assert.Equal(t, "$0.0000", usage.TotalCost())

	usage = objstore.S3Usage{}
	for range 1_000 {
		usage.AddExpensiveRequest()
	}
	assert.Equal(t, "$0.0050", usage.TotalCost())
}
