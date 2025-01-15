package jobs

import (
	"time"

	"reduction.dev/reduction/clocks"
)

type LivenessTracker struct {
	m        map[string]time.Time
	clock    clocks.Clock
	deadline time.Duration
}

func NewLivenessTracker(clock clocks.Clock, deadline time.Duration) *LivenessTracker {
	return &LivenessTracker{
		m:        make(map[string]time.Time, 0),
		clock:    clock,
		deadline: deadline,
	}
}

func (lt *LivenessTracker) Heartbeat(id string) {
	lt.m[id] = lt.clock.Now()
}

func (lt *LivenessTracker) Purge() []string {
	var missing []string
	for id, hb := range lt.m {
		if hb.Before(lt.clock.Now().Add(-lt.deadline)) {
			missing = append(missing, id)
			delete(lt.m, id)
		}
	}
	return missing
}
