package connectors_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"reduction.dev/reduction/connectors"
)

func TestIsRetryable(t *testing.T) {
	// Regular errors default to retryable
	err := errors.New("some error")
	assert.True(t, connectors.IsRetryable(err))

	// Explicitly marked as retryable
	retryableErr := connectors.NewRetryableError(errors.New("temporary problem"))
	assert.True(t, connectors.IsRetryable(retryableErr))

	// Explicitly marked as non-retryable
	nonRetryableErr := connectors.NewTerminalError(errors.New("configuration issue"))
	assert.False(t, connectors.IsRetryable(nonRetryableErr))

	// Wrapped errors maintain retryability
	wrappedErr := fmt.Errorf("context: %w", nonRetryableErr)
	assert.False(t, connectors.IsRetryable(wrappedErr))
}
