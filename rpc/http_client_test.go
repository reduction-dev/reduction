package rpc_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"reduction.dev/reduction/rpc"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPostRetry(t *testing.T) {
	attempts := 0
	lastPayload := ""
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		bodyBytes, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		lastPayload = string(bodyBytes)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("500 unknown error"))
	}))
	defer s.Close()

	client := rpc.NewHTTPClient(rpc.WithInitialRetryDelay(0))

	httpReq, err := http.NewRequestWithContext(context.Background(), "POST", s.URL, strings.NewReader("hello"))
	require.NoError(t, err)

	client.Do(httpReq)

	assert.Equal(t, 10, attempts)
	assert.Equal(t, "hello", lastPayload)
}
