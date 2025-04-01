package rpc_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"reduction.dev/reduction/rpc"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRetryOn503(t *testing.T) {
	attempts := 0
	lastPayload := ""
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Succeed after 3 attempts
		if attempts > 3 {
			w.WriteHeader(http.StatusOK)
			return
		}
		attempts++
		bodyBytes, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		lastPayload = string(bodyBytes)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer s.Close()

	client := rpc.NewHTTPClient("dummy", slog.Default(), rpc.WithInitialRetryDelay(0))

	httpReq, err := http.NewRequestWithContext(context.Background(), "POST", s.URL, strings.NewReader("hello"))
	require.NoError(t, err)

	_, _ = client.Do(httpReq)

	assert.Greater(t, attempts, 3) // Should have retried multiple times
	assert.Equal(t, "hello", lastPayload)
}

func TestNoRetryOn404(t *testing.T) {
	attempts := 0
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		w.WriteHeader(http.StatusNotFound)
	}))
	defer s.Close()

	client := rpc.NewHTTPClient("dummy", slog.Default(), rpc.WithInitialRetryDelay(0))

	httpReq, err := http.NewRequestWithContext(context.Background(), "GET", s.URL, nil)
	require.NoError(t, err)

	resp, err := client.Do(httpReq)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Equal(t, 1, attempts) // Should not have retried
}

func TestNetworkErrorHandlingFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start with a server that returns 503
	serverAttempts := 0
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serverAttempts++
		// Return 503 on first request to trigger retry loop
		if serverAttempts == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}

		// Close connection without response to simulate network failure
		hj, ok := w.(http.Hijacker)
		require.True(t, ok, "webserver doesn't support hijacking")
		conn, _, err := hj.Hijack()
		require.NoError(t, err)
		conn.Close() // Close the connection without sending a response

		// Cancel the context to stop the test
		cancel()
	}))
	defer s.Close()

	client := rpc.NewHTTPClient("dummy", slog.Default(), rpc.WithInitialRetryDelay(0))

	httpReq, err := http.NewRequestWithContext(ctx, "GET", s.URL, nil)
	require.NoError(t, err)

	// This should not panic even though we'll get a nil response during retry
	_, err = client.Do(httpReq)

	// Should fail with error
	assert.Error(t, err)
	assert.Equal(t, 2, serverAttempts, "Should have attempted exactly 2 requests")
}

func TestNetworkErrorHandlingSuccess(t *testing.T) {
	serverAttempts := 0
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serverAttempts++

		switch serverAttempts {
		case 1:
			// First request: return 503 to enter retry loop
			w.WriteHeader(http.StatusServiceUnavailable)
		case 2, 3, 4:
			// Next few requests: simulate network error
			hj, ok := w.(http.Hijacker)
			require.True(t, ok)
			conn, _, err := hj.Hijack()
			require.NoError(t, err)
			conn.Close()
		default:
			// Eventually succeed
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("success"))
		}
	}))
	defer s.Close()

	client := rpc.NewHTTPClient("dummy", slog.Default(), rpc.WithInitialRetryDelay(0))

	httpReq, err := http.NewRequestWithContext(context.Background(), "GET", s.URL, nil)
	require.NoError(t, err)

	resp, err := client.Do(httpReq)

	// Should eventually succeed
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.Equal(t, "success", string(body))

	assert.Equal(t, 5, serverAttempts, "Should have made 5 requests total")
}
