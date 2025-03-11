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
