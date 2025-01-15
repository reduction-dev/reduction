package refs_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"reduction.dev/reduction/dkv/refs"
)

func TestHoldingAndDropping(t *testing.T) {
	rc := &refs.RefCount{}
	retained := rc.Hold()
	assert.True(t, retained, "first hold returns retained")
	retained = rc.Hold()
	assert.False(t, retained, "second hold return not retained")

	released := rc.Drop()
	assert.False(t, released, "first drop isn't released")
	released = rc.Drop()
	assert.True(t, released, "second drop is released")

	assert.Panics(t, func() {
		rc.Drop()
	}, "release beyond 0 panics")
}
