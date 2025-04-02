package rpc

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"time"

	"reduction.dev/reduction/util/httpu"
)

type HTTPClient struct {
	initialRetryDelay int
	goHTTPClient      *http.Client
	logger            *slog.Logger
}

type newOption func(c *HTTPClient)

func NewHTTPClient(metricName string, logger *slog.Logger, options ...newOption) *HTTPClient {
	client := &HTTPClient{
		initialRetryDelay: 100, // Default to 100ms if not specified
		goHTTPClient:      httpu.NewClient(metricName),
		logger:            logger,
	}
	for _, o := range options {
		o(client)
	}
	return client
}

func WithInitialRetryDelay(delay int) newOption {
	return func(c *HTTPClient) {
		c.initialRetryDelay = delay
	}
}

// Do performs an HTTP request with automatic retries for certain errors.
func (c *HTTPClient) Do(req *http.Request) (*http.Response, error) {
	// Try the initial request
	resp, err := c.goHTTPClient.Do(req)

	// Handle network errors like DNS resolution failures
	if err != nil {
		if isRetryableError(err) {
			return c.retryRequest(req, nil, err)
		}

		return nil, err
	}

	if isRetryableErrorStatus(resp.StatusCode) {
		return c.retryRequest(req, resp, nil)
	}

	return resp, nil
}

// retryRequest handles the retry loop for requests that need to be retried
func (c *HTTPClient) retryRequest(req *http.Request, firstResp *http.Response, initialErr error) (*http.Response, error) {
	currentResp := firstResp
	currentErr := initialErr
	retryCount := 0
	initialDelay := time.Duration(c.initialRetryDelay) * time.Millisecond
	const maxDelay = 10 * time.Second

	for {
		// Try to get info from response for logging
		var errDetails string
		var retryReason string
		if currentResp == nil {
			retryReason = "network error"
			errDetails = currentErr.Error()
		} else {
			errDetails = readBodyMessage(currentResp)
			retryReason = currentResp.Status
		}

		// Log first retry (retryCount=0) and then every 10 retries
		if retryCount%10 == 0 {
			c.logger.Info("Continuing to retry request",
				"url", req.URL.String(),
				"retryReason", retryReason,
				"retryCount", retryCount,
				"errDetails", errDetails)
		}

		retryCount++

		// Calculate delay with exponential backoff capped at maxDelay
		delay := min(initialDelay*time.Duration(retryCount), maxDelay)

		select {
		case <-req.Context().Done():
			return nil, fmt.Errorf("request context error: %w", req.Context().Err())
		case <-time.After(delay):
			// Continue
		}

		// Reset the request body if needed
		if err := resetRequestBody(req); err != nil {
			return nil, err
		}

		// Retry the request
		var err error
		currentResp, err = c.goHTTPClient.Do(req)

		if err != nil {
			if isRetryableError(err) {
				currentErr = err
				continue
			}
			return currentResp, err
		}

		if isRetryableErrorStatus(currentResp.StatusCode) {
			continue
		}

		// Return with success or terminal error
		return currentResp, err
	}
}

// resetRequestBody resets the request body for retrying the request
func resetRequestBody(req *http.Request) error {
	if req.Body == nil {
		return nil
	}

	newBody, err := req.GetBody()
	if err != nil {
		return fmt.Errorf("failed to reset request body: %w", err)
	}

	req.Body = newBody
	return nil
}

func readBodyMessage(resp *http.Response) string {
	if resp.Body == nil {
		return ""
	}
	bodyBytes, readErr := io.ReadAll(resp.Body)
	resp.Body.Close()
	if readErr != nil || len(bodyBytes) == 0 {
		return ""
	}
	resp.Body = io.NopCloser(bytes.NewReader(bodyBytes))
	return string(bodyBytes)
}

// isRetryableErrorStatus returns whether a status code should be retried indefinitely
func isRetryableErrorStatus(statusCode int) bool {
	switch statusCode {
	case
		http.StatusRequestTimeout,     // 408
		http.StatusTooManyRequests,    // 429
		http.StatusBadGateway,         // 502
		http.StatusServiceUnavailable, // 503
		http.StatusGatewayTimeout:     // 504
		return true
	}
	return false
}

// isRetryableError determines whether a network error should be retried
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	// Retry DNS lookup errors
	var dnsError *net.DNSError
	if errors.As(err, &dnsError) {
		// Retry temporary network errors
		if dnsError.IsNotFound {
			return true
		}
	}

	// Retry closed connections
	if errors.Is(err, io.EOF) {
		return true
	}

	return false
}
