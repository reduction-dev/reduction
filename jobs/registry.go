package jobs

import (
	"errors"
	"fmt"
	"log/slog"

	"reduction.dev/reduction/proto"
	"reduction.dev/reduction/proto/jobpb"
	"reduction.dev/reduction/util/ds"
)

var ErrNotEnoughResources = errors.New("not enough resources")

type Registry struct {
	runners   *ds.SortedMap[string, proto.SourceRunner]
	operators *ds.SortedMap[string, proto.Operator]
	taskCount int
	liveness  *LivenessTracker
}

func NewRegistry(taskCount int, liveness *LivenessTracker) *Registry {
	return &Registry{
		runners:   ds.NewSortedMap[string, proto.SourceRunner](),
		operators: ds.NewSortedMap[string, proto.Operator](),
		taskCount: taskCount,
		liveness:  liveness,
	}
}

func (r *Registry) RegisterSourceRunner(runner proto.SourceRunner) {
	r.liveness.Heartbeat(runner.ID())
	r.runners.Set(runner.ID(), runner)
}

func (r *Registry) DeregisterSourceRunner(sr *jobpb.NodeIdentity) {
	r.runners.Delete(sr.Id)
}

func (r *Registry) RegisterOperator(op proto.Operator) {
	r.liveness.Heartbeat(op.ID())
	r.operators.Set(op.ID(), op)
}

func (r *Registry) DeregisterOperator(op *jobpb.NodeIdentity) {
	r.operators.Delete(op.Id)
}

// NewAssembly attempts to get the required operators and source runners
// for an assembly. Returns nil, ErrNotEnoughResources if insufficient resources.
func (r *Registry) NewAssembly() (*Assembly, error) {
	if r.runners.Size() < r.taskCount {
		return nil, fmt.Errorf("need %d source runners but had %d registered: %w", r.taskCount, r.runners.Size(), ErrNotEnoughResources)
	}

	if r.operators.Size() < r.taskCount {
		return nil, fmt.Errorf("need %d operators but had %d registered: %w", r.taskCount, r.operators.Size(), ErrNotEnoughResources)
	}

	assembly := NewAssembly(r.operators.Values()[:r.taskCount], r.runners.Values()[:r.taskCount])
	return assembly, nil
}

// Purge removes any dead nodes and returns their IDs
func (r *Registry) Purge() []string {
	var purged []string
	for _, id := range r.liveness.Purge() {
		purged = append(purged, id)
		r.runners.Delete(id)
		r.operators.Delete(id)
	}

	return purged
}

func (r *Registry) Diagnostics() []any {
	return []any{
		slog.Int("desiredTaskCount", r.taskCount),
		slog.Int("sourceRunnerCount", r.runners.Size()),
		slog.Int("operatorCount", r.operators.Size()),
	}
}

func (r *Registry) HasSourceRunner(sr proto.SourceRunner) bool {
	return r.runners.Has(sr.ID())
}

func (r *Registry) HasOperator(op proto.Operator) bool {
	return r.operators.Has(op.ID())
}
