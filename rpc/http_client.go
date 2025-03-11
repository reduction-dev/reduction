package rpc

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
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

// isRetryStatus returns whether a status code should be retried indefinitely
func isRetryStatus(statusCode int) bool {
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

func (c *HTTPClient) Do(req *http.Request) (*http.Response, error) {
	resp, err := c.goHTTPClient.Do(req)
	if err != nil {
		return nil, err
	}

	// Return immediately if status is not retry-able
	if !isRetryStatus(resp.StatusCode) {
		return resp, nil
	}

	// Handle retry logic for retryable status codes
	return c.retryRequest(req, resp)
}

// retryRequest handles the retry loop for requests that need to be retried
func (c *HTTPClient) retryRequest(req *http.Request, firstResp *http.Response) (*http.Response, error) {
	resp := firstResp
	bodyMsg := readBodyMessage(resp)
	retryCount := 0

	initialDelay := time.Duration(c.initialRetryDelay) * time.Millisecond
	const maxDelay = 10 * time.Second

	for {
		retryCount++

		// Log every 10 retries
		if retryCount%10 == 0 {
			c.logger.Info("Continuing to retry request",
				"url", req.URL.String(),
				"status", resp.Status,
				"retryCount", retryCount,
				"responseBody", bodyMsg)
		}

		// Calculate delay with exponential backoff capped at maxDelay
		delay := min(initialDelay*time.Duration(retryCount), maxDelay)
		time.Sleep(delay)

		// Reset the request body if needed
		if err := resetRequestBody(req); err != nil {
			return nil, err
		}

		// Retry the request
		var err error
		resp, err = c.goHTTPClient.Do(req)
		if err != nil {
			continue
		}

		// If status is no longer retryable, we can return
		if !isRetryStatus(resp.StatusCode) {
			return resp, nil
		}

		// Update bodyMsg for next iteration's logging
		bodyMsg = readBodyMessage(resp)
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
