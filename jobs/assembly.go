package jobs

import (
	"context"
	"fmt"
	"log/slog"

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
	logger        *slog.Logger
	keySpace      *partitioning.KeySpace
}

func NewAssembly(
	operators []proto.Operator,
	sourceRunners []proto.SourceRunner,
	log *slog.Logger,
	keySpace *partitioning.KeySpace,
) *Assembly {
	return &Assembly{
		sourceRunners: sourceRunners,
		operators:     operators,
		logger:        log,
		keySpace:      keySpace,
	}
}

// Healthy checks whether all nodes in the assembly are still active
func (a *Assembly) Healthy(registry NodeRegistry) bool {
	for _, sr := range a.sourceRunners {
		if !registry.HasSourceRunner(sr) {
			a.logger.Info("assembly not healthy", "missing-source-runner", sr.ID())
			return false
		}
	}

	for _, op := range a.operators {
		if !registry.HasOperator(op) {
			a.logger.Info("assembly not healthy", "missing-operator", op.ID())
			return false
		}
	}

	return true
}

// Start tells the assembly of nodes to begin working from a checkpoint
func (a *Assembly) Start(cfg *config.Config, ckpt *snapshotpb.JobCheckpoint, splitAssignments map[string][]*workerpb.SourceSplit) error {
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
			return sr.Start(gctx, &workerpb.StartSourceRunnerRequest{
				Splits:        splitAssignments[sr.ID()],
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

	opCkptAssignments := partitioning.AssignRanges(a.keySpace.KeyGroupRanges(), ckptRanges)
	for i, op := range a.operators {
		eg.Go(func() error {
			return op.Start(gctx, &workerpb.StartOperatorRequest{
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
