package connectors

import (
	"errors"
)

var ErrEndOfInput = errors.New("end of input")

// SourceError wraps errors from source readers and indicates if they're retryable
type SourceError struct {
	Err       error
	Retryable bool
}

func (e *SourceError) Error() string {
	return e.Err.Error()
}

func (e *SourceError) Unwrap() error {
	return e.Err
}

// NewRetryableError wraps an error as retryable
func NewRetryableError(err error) *SourceError {
	return &SourceError{
		Err:       err,
		Retryable: true,
	}
}

// NewTerminalError wraps an error as non-retryable
func NewTerminalError(err error) *SourceError {
	return &SourceError{
		Err:       err,
		Retryable: false,
	}
}

// IsRetryable checks if an error is retryable
func IsRetryable(err error) bool {
	var sourceErr *SourceError
	if errors.As(err, &sourceErr) {
		return sourceErr.Retryable
	}

	// Retry if not explicitly marked as non-retryable
	return true
}
