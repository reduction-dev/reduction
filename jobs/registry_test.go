package jobs_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"reduction.dev/reduction/clocks"
	"reduction.dev/reduction/jobs"
	"reduction.dev/reduction/proto"
	"reduction.dev/reduction/proto/jobpb"
)

func TestRegistry_HasChangesAfterSourceRunnerRegistration(t *testing.T) {
	liveness := jobs.NewLivenessTracker(clocks.NewFrozenClock(), time.Second)
	registry := jobs.NewRegistry(1, liveness)

	assert.False(t, registry.HasChanges(), "no changes initially")

	registry.RegisterSourceRunner(&SourceRunnerStub{id: "sr", host: "host"})
	assert.True(t, registry.HasChanges(), "changed after registration")

	registry.AcknowledgeChanges()
	assert.False(t, registry.HasChanges(), "no changes after acknowledgement")

	registry.RegisterSourceRunner(&SourceRunnerStub{id: "sr", host: "host"})
	assert.False(t, registry.HasChanges(), "registering same source runner again should not change")

	registry.DeregisterSourceRunner(&jobpb.NodeIdentity{Id: "sr"})
	assert.True(t, registry.HasChanges(), "changed after deregistration")
}

func TestRegistry_HasChangesAfterOperatorRegistration(t *testing.T) {
	liveness := jobs.NewLivenessTracker(clocks.NewFrozenClock(), time.Second)
	registry := jobs.NewRegistry(1, liveness)

	assert.False(t, registry.HasChanges(), "no changes initially")

	registry.RegisterOperator(&OperatorStub{id: "op", host: "host"})
	assert.True(t, registry.HasChanges(), "changed after registration")

	registry.AcknowledgeChanges()
	assert.False(t, registry.HasChanges(), "no changes after acknowledgement")

	registry.RegisterOperator(&OperatorStub{id: "op", host: "host"})
	assert.False(t, registry.HasChanges(), "registering same operator again should not change")

	registry.DeregisterOperator(&jobpb.NodeIdentity{Id: "op"})
	assert.True(t, registry.HasChanges(), "changed after deregistration")
}

func TestRegistry_HasChangesAfterPurge(t *testing.T) {
	// Create a registry with a clock we can advance
	clock := clocks.NewFrozenClock()
	liveness := jobs.NewLivenessTracker(clock, time.Second*5)
	registry := jobs.NewRegistry(1, liveness)

	// Register an operator and source runner
	registry.RegisterOperator(&OperatorStub{id: "op", host: "host"})
	runner := &SourceRunnerStub{id: "sr", host: "host"}
	registry.RegisterSourceRunner(runner)
	registry.AcknowledgeChanges()

	assert.False(t, registry.HasChanges(), "no changes after acknowledgement")

	// Advance clock past heartbeat deadline
	clock.Advance(time.Second * 10)

	// Purge should remove dead nodes and mark changes
	purged := registry.Purge()
	assert.Len(t, purged, 2)
	assert.True(t, registry.HasChanges(), "changes after purge")

	// Purge with no dead nodes should not mark changes
	registry.AcknowledgeChanges()
	purged = registry.Purge()
	assert.Len(t, purged, 0)
	assert.False(t, registry.HasChanges(), "no changes after purge with no dead nodes")
}

// SourceRunnerStub implements proto.SourceRunner for testing.
type SourceRunnerStub struct {
	*proto.UnimplementedSourceRunner
	id   string
	host string
}

func (r *SourceRunnerStub) ID() string {
	return r.id
}

func (r *SourceRunnerStub) Host() string {
	return r.host
}

// OperatorStub implements proto.Operator for testing.
type OperatorStub struct {
	*proto.UnimplementedOperator
	id   string
	host string
}

func (o *OperatorStub) ID() string {
	return o.id
}

func (o *OperatorStub) Host() string {
	return o.host
}
