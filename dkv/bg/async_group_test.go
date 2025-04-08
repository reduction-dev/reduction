package bg_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"reduction.dev/reduction/dkv/bg"
)

func TestRunningTasksSerially(t *testing.T) {
	g := bg.NewAsyncGroup()
	limit := 30
	q := bg.NewQueue(limit)
	c := make(chan int, limit)

	expected := make([]int, limit)
	for i := range limit {
		expected[i] = i
		g.Enqueue(q, func() error {
			c <- i
			return nil
		})
	}
	require.NoError(t, g.Wait())
	close(c)

	actual := make([]int, 0, limit)
	for i := range c {
		actual = append(actual, i)
	}

	assert.Equal(t, expected, actual)
}
