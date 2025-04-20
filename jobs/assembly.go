package jobs

import (
	"context"
	"fmt"

	"golang.org/x/sync/errgroup"
	"reduction.dev/reduction-protocol/jobconfigpb"
	"reduction.dev/reduction/config"
	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/partitioning"
	"reduction.dev/reduction/proto"
	"reduction.dev/reduction/proto/jobpb"
	"reduction.dev/reduction/proto/snapshotpb"
	"reduction.dev/reduction/proto/workerpb"
	"reduction.dev/reduction/util/sliceu"
)

type Assembly struct {
	sourceRunners []proto.SourceRunner
	operators     []proto.Operator
}

func NewAssembly(
	operators []proto.Operator,
	sourceRunners []proto.SourceRunner,
) *Assembly {
	return &Assembly{
		sourceRunners: sourceRunners,
		operators:     operators,
	}
}

// Healthy checks whether all nodes in the assembly are still active
// Returns true if healthy, or false with a reason if unhealthy
func (a *Assembly) Healthy(registry NodeRegistry) (bool, string) {
	for _, sr := range a.sourceRunners {
		if !registry.HasSourceRunner(sr) {
			return false, fmt.Sprintf("missing source runner: %s", sr.ID())
		}
	}

	for _, op := range a.operators {
		if !registry.HasOperator(op) {
			return false, fmt.Sprintf("missing operator: %s", op.ID())
		}
	}

	return true, ""
}

// Start tells the assembly of nodes to begin working from a checkpoint
func (a *Assembly) Deploy(cfg *config.Config, ckpt *snapshotpb.JobCheckpoint) error {
	// Gather attributes source runner messages
	srIdentities := make([]*jobpb.NodeIdentity, len(a.sourceRunners))
	srIDs := make([]string, len(a.sourceRunners))
	for i, sr := range a.sourceRunners {
		srIdentities[i] = &jobpb.NodeIdentity{Id: sr.ID(), Host: sr.Host()}
		srIDs[i] = sr.ID()
	}

	// Gather attributes for operators
	opIdentities := make([]*jobpb.NodeIdentity, len(a.operators))
	for i, op := range a.operators {
		opIdentities[i] = &jobpb.NodeIdentity{Id: op.ID(), Host: op.Host()}
	}

	eg, gctx := errgroup.WithContext(context.Background())
	for _, sr := range a.sourceRunners {
		eg.Go(func() error {
			return sr.Deploy(gctx, &workerpb.DeploySourceRunnerRequest{
				Operators:     opIdentities,
				KeyGroupCount: int32(cfg.KeyGroupCount),
				Sources: sliceu.Map(cfg.Sources, func(s connectors.SourceConfig) *jobconfigpb.Source {
					return s.ProtoMessage()
				}),
			})
		})
	}

	// Create list of ranges from checkpoints
	ckptRanges := make([]partitioning.KeyGroupRange, len(ckpt.GetOperatorCheckpoints()))
	for i, opCkpt := range ckpt.GetOperatorCheckpoints() {
		ckptRanges[i] = partitioning.KeyGroupRangeFromProto(opCkpt.KeyGroupRange)
	}

	opCkptAssignments := partitioning.AssignRanges(cfg.KeySpace().KeyGroupRanges(), ckptRanges)
	for i, op := range a.operators {
		eg.Go(func() error {
			return op.Deploy(gctx, &workerpb.DeployOperatorRequest{
				Operators:       opIdentities,
				SourceRunnerIds: srIDs,
				Checkpoints:     sliceu.Pick(ckpt.GetOperatorCheckpoints(), opCkptAssignments[i]),
				KeyGroupCount:   int32(cfg.KeyGroupCount),
				Sinks: sliceu.Map(cfg.Sinks, func(s connectors.SinkConfig) *jobconfigpb.Sink {
					return s.ProtoMessage()
				}),
				StorageLocation: cfg.WorkingStorageLocation,
			})
		})
	}

	return eg.Wait()
}

func (a *Assembly) AssignSplits(splits map[string][]*workerpb.SourceSplit) error {
	g, gctx := errgroup.WithContext(context.Background())
	for _, sr := range a.sourceRunners {
		g.Go(func() error {
			return sr.AssignSplits(gctx, splits[sr.ID()])
		})
	}
	return g.Wait()
}

func (a *Assembly) StartCheckpoint(ctx context.Context, id uint64) error {
	g, gctx := errgroup.WithContext(ctx)
	for _, sr := range a.sourceRunners {
		g.Go(func() error {
			return sr.StartCheckpoint(gctx, id)
		})
	}
	return g.Wait()
}

func (a *Assembly) UpdateRetainedCheckpoints(ctx context.Context, ids []uint64) error {
	g, gctx := errgroup.WithContext(ctx)
	for _, op := range a.operators {
		g.Go(func() error {
			return op.UpdateRetainedCheckpoints(gctx, ids)
		})
	}
	return g.Wait()
}

func (a *Assembly) SourceRunnerIDs() []string {
	ids := make([]string, len(a.sourceRunners))
	for i, sr := range a.sourceRunners {
		ids[i] = sr.ID()
	}
	return ids
}

func (a *Assembly) OperatorIDs() []string {
	ids := make([]string, len(a.operators))
	for i, op := range a.operators {
		ids[i] = op.ID()
	}
	return ids
}

func (a *Assembly) String() string {
	if a == nil {
		return "none"
	}
	return fmt.Sprintf("operators: %v, source_runners: %v", a.OperatorIDs(), a.SourceRunnerIDs())
}

// NodeRegistry defines the minimal interface the Assembly needs from Registry
type NodeRegistry interface {
	HasSourceRunner(sr proto.SourceRunner) bool
	HasOperator(op proto.Operator) bool
}
