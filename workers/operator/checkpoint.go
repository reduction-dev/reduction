package operator

import (
	"fmt"

	"reduction.dev/reduction/proto/workerpb"
	"reduction.dev/reduction/util/sliceu"
)

type checkpoint struct {
	checkpointID        uint64
	srIDs               map[string]struct{}
	allBarriersReceived chan struct{}
}

func newCheckpoint(checkpointID uint64, srIDs []string) *checkpoint {
	return &checkpoint{
		checkpointID:        checkpointID,
		srIDs:               sliceu.KeyMap(srIDs),
		allBarriersReceived: make(chan struct{}),
	}
}

func (c *checkpoint) registerBarrier(senderID string, barrier *workerpb.CheckpointBarrier) error {
	if barrier.CheckpointId != c.checkpointID {
		return fmt.Errorf("checkpoint ID mismatch, had %d got %d", c.checkpointID, barrier.CheckpointId)
	}
	delete(c.srIDs, senderID)
	if len(c.srIDs) == 0 {
		close(c.allBarriersReceived)
	}
	return nil
}

func (c *checkpoint) hasAllBarriers() bool {
	return len(c.srIDs) == 0
}

// alignSender returns a waiter function that will block requests from senders
// until all barriers have been received.
func (c *checkpoint) alignSender(senderID string) (wait func()) {
	if c == nil {
		return func() {} // no-op if there is no checkpoint
	}

	if _, ok := c.srIDs[senderID]; !ok {
		return func() {
			<-c.allBarriersReceived
		}
	}
	return func() {}
}
