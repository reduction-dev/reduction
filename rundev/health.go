package rundev

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"reduction.dev/reduction/util/netu"
)

// Check the handler /health endpoint to know when it's ready to receive requests.
func waitUntilHealthy(ctx context.Context, addr string, retryInterval time.Duration, maxWaitTime time.Duration) error {
	client := &http.Client{Timeout: 1 * time.Second}
	deadline := time.Now().Add(maxWaitTime)
	baseURL, err := netu.ResolveAddr(addr)
	if err != nil {
		return err
	}
	healthEndpoint := baseURL + "/health"

	callEndpoint := func() (status int, err error) {
		resp, err := client.Get(healthEndpoint)
		if err != nil {
			return 0, err
		}
		defer resp.Body.Close()
		return resp.StatusCode, nil
	}

	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		default:
			status, err := callEndpoint()

			// Health check successful, done.
			if status == http.StatusOK {
				return nil
			}

			// Otherwise retry or fail
			if time.Now().After(deadline) {
				return fmt.Errorf("timed out after %v; err=%v status=%v", maxWaitTime, err, status)
			}
			time.Sleep(retryInterval)
		}
	}
}
