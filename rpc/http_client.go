package rpc

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"reduction.dev/reduction/util/httpu"
)

type HTTPClient struct {
	initialRetryDelay int
	goHTTPClient      *http.Client
}

type newOption func(c *HTTPClient)

func NewHTTPClient(metricName string, options ...newOption) *HTTPClient {
	client := &HTTPClient{
		goHTTPClient: httpu.NewClient(metricName),
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

func (c *HTTPClient) Do(req *http.Request) (*http.Response, error) {
	var resp *http.Response
	err := retry(10, 100*time.Millisecond, func(isRetry bool) error {
		// Reset the body if present, otherwise will be 0 (already read)
		if isRetry && req.Body != nil {
			var err error
			req.Body, err = req.GetBody()
			if err != nil {
				return err
			}
		}

		var err error
		resp, err = c.goHTTPClient.Do(req)
		if err != nil {
			return err
		}

		if resp.StatusCode == 0 || (resp.StatusCode >= 500 && resp.StatusCode != http.StatusNotImplemented) || resp.StatusCode == http.StatusTooManyRequests {
			return fmt.Errorf("status=%q url=%s: %w", resp.Status, req.URL.String(), errRetry)
		}

		return nil
	})

	return resp, err
}

func retry(attempts int, initialDelay time.Duration, fn func(isRetry bool) error) error {
	delay := initialDelay
	var err error
	for i := 0; i < attempts; i++ {
		if i > 0 {
			time.Sleep(delay * time.Duration(i))
		}
		err = fn(i > 0)
		if errors.Is(err, errRetry) {
			continue
		}
		return err
	}
	return fmt.Errorf("after %d attempts, last error: %w", attempts, err)
}

var errRetry = errors.New("retryable")
