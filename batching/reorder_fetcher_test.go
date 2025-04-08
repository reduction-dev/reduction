package batching

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReorderFetcher(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const eventCount = 100

	errChan := make(chan error, eventCount)

	rf := NewReorderFetcher(ctx, NewReorderFetcherParams[string, string]{
		Batcher: NewEventBatcher[string](ctx, EventBatcherParams{MaxDelay: time.Minute, MaxSize: 2}),
		FetchBatch: func(ctx context.Context, events []string) ([]string, error) {
			return events, nil
		},
		BufferSize: eventCount,
		ErrChan:    errChan,
	})

	// Use AsyncBatcher.Add so flush occurs when batch is full.
	for i := range eventCount {
		rf.Add(ctx, strconv.Itoa(i))
	}

	var results []string
	for i := range eventCount {
		select {
		case res := <-rf.Output:
			t.Log("output", i, "res", res)
			results = append(results, res)
		case err := <-errChan:
			require.Fail(t, err.Error())
		case <-ctx.Done():
			require.Fail(t, "timeout waiting for batch responses")
		}
	}

	require.Len(t, results, eventCount)
	for i := range eventCount {
		assert.Equal(t, strconv.Itoa(i), results[i])
	}
}
