package jobs_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"reduction.dev/reduction/clocks"
	"reduction.dev/reduction/jobs"
)

func TestLivenessTracker_Purge(t *testing.T) {
	c := clocks.NewFrozenClock()
	lt := jobs.NewLivenessTracker(c, time.Second*1)

	// Two nodes in the cluster
	lt.Heartbeat("1")
	lt.Heartbeat("2")
	assert.Len(t, lt.Purge(), 0)

	// Second passes and only node 2 heartbeats
	c.Advance(time.Second * 1)
	lt.Heartbeat("2")
	c.Advance(time.Second * 1)

	assert.Equal(t, []string{"1"}, lt.Purge())
}
