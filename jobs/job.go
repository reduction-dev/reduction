package jobs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"reduction.dev/reduction/clocks"
	"reduction.dev/reduction/config"
	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/partitioning"
	"reduction.dev/reduction/proto"
	"reduction.dev/reduction/proto/jobpb"
	"reduction.dev/reduction/proto/snapshotpb"
	"reduction.dev/reduction/storage"
	"reduction.dev/reduction/storage/localfs"
	"reduction.dev/reduction/storage/snapshots"
	"reduction.dev/reduction/util/sliceu"
)

type Job struct {
	snapshotStore       *snapshots.Store
	log                 *slog.Logger
	stateUpdates        chan func()
	assembly            *Assembly
	sourceSplitter      connectors.SourceSplitter
	registry            *Registry
	config              *config.Config
	clock               clocks.Clock
	checkpointTicker    *clocks.Ticker
	keySpace            *partitioning.KeySpace
	operatorFactory     proto.OperatorFactory
	sourceRunnerFactory proto.SourceRunnerFactory
}

type NewParams struct {
	JobConfig           *config.Config
	SavepointURI        string
	Clock               clocks.Clock
	HeartbeatDeadline   time.Duration
	Store               storage.FileStore
	CheckpointsPath     string
	SavepointsPath      string
	Logger              *slog.Logger
	OperatorFactory     proto.OperatorFactory
	SourceRunnerFactory proto.SourceRunnerFactory
	CheckpointEvents    chan snapshots.CheckpointEvent
}

func New(params *NewParams) *Job {
	// Default KeyGroupCount to 256
	if params.JobConfig.KeyGroupCount == 0 {
		params.JobConfig.KeyGroupCount = 256
	}

	// Default Heartbeat deadline to 5s
	if params.HeartbeatDeadline == 0 {
		params.HeartbeatDeadline = time.Second * 5
	}

	// Default to system clock
	if params.Clock == nil {
		params.Clock = clocks.NewSystemClock()
	}

	// Default checkpoint storage to ./
	if params.Store == nil {
		params.Store = localfs.NewInWorkingDirectory("")
	}

	// Default CheckpointsPath to ./checkpoints
	if params.CheckpointsPath == "" {
		params.CheckpointsPath = "checkpoints"
	}

	// Default SavepointsPath to ./savepoints
	if params.SavepointsPath == "" {
		params.SavepointsPath = "savepoints"
	}

	// Provide default logger
	if params.Logger == nil {
		params.Logger = slog.With("instanceID", "job")
	}

	snapshotStore := snapshots.NewStore(&snapshots.NewStoreParams{
		SavepointURI:     params.SavepointURI,
		FileStore:        params.Store,
		SavepointsPath:   params.SavepointsPath,
		CheckpointsPath:  params.CheckpointsPath,
		CheckpointEvents: params.CheckpointEvents,
	})
	job := &Job{
		snapshotStore:       snapshotStore,
		log:                 params.Logger,
		stateUpdates:        make(chan func()),
		registry:            NewRegistry(params.JobConfig.WorkerCount, NewLivenessTracker(params.Clock, params.HeartbeatDeadline)),
		config:              params.JobConfig,
		clock:               params.Clock,
		sourceSplitter:      params.JobConfig.Sources[0].NewSourceSplitter(),
		keySpace:            partitioning.NewKeySpace(params.JobConfig.KeyGroupCount, params.JobConfig.WorkerCount),
		operatorFactory:     params.OperatorFactory,
		sourceRunnerFactory: params.SourceRunnerFactory,
	}

	go job.processStateUpdates()

	return job
}

func (j *Job) HandleRegisterOperator(node *jobpb.NodeIdentity) {
	j.stateUpdates <- func() {
		j.log.Info("registering operator", "id", node.Id, "host", node.Host)
		operator := j.operatorFactory("job", node, nil)
		j.registry.RegisterOperator(operator)
	}
}

func (j *Job) HandleDeregisterOperator(op *jobpb.NodeIdentity) {
	j.stateUpdates <- func() {
		j.log.Info("deregistered operator", "id", op.Id, "host", op.Host)
		j.registry.DeregisterOperator(op)
	}
}

func (j *Job) HandleRegisterSourceRunner(node *jobpb.NodeIdentity) {
	j.stateUpdates <- func() {
		j.log.Info("registered source runner", "id", node.Id, "host", node.Host)
		sr := j.sourceRunnerFactory(node)
		j.registry.RegisterSourceRunner(sr)
	}
}

func (j *Job) HandleDeregisterSourceRunner(sr *jobpb.NodeIdentity) {
	j.stateUpdates <- func() {
		j.log.Info("deregistered source runner", "id", sr.Id, "host", sr.Host)
		j.registry.DeregisterSourceRunner(sr)
	}
}

func (j *Job) HandleCreateSavepoint(ctx context.Context) (uint64, error) {
	// Create a pending savepoint in the snapshot store to track savepointing.
	checkpointID, created, err := j.snapshotStore.CreateSavepoint(j.assembly.OperatorIDs(), j.assembly.SourceRunnerIDs())
	if err != nil {
		return 0, fmt.Errorf("failed creating savepoint: %v", err)
	}

	// Only start the checkpoint if it's new. Otherwise the checkpoint was already in-flight.
	if created {
		if err := j.assembly.StartCheckpoint(ctx, checkpointID); err != nil {
			return 0, fmt.Errorf("failed starting snapshot: %w", err)
		}
	}

	return checkpointID, nil
}

func (j *Job) HandleGetSavepointURI(ctx context.Context, savepointID uint64) (string, error) {
	return j.snapshotStore.SavepointURIForID(savepointID)
}

func (j *Job) HandleOperatorCheckpointComplete(ctx context.Context, req *snapshotpb.OperatorCheckpoint) error {
	return j.snapshotStore.AddOperatorSnapshot(req)
}

func (j *Job) HandleSourceCheckpointComplete(ctx context.Context, snapshot *snapshotpb.SourceCheckpoint) error {
	return j.snapshotStore.AddSourceSnapshot(snapshot)
}

// Evaluate the cluster state and transition to paused or running. This
// method and others that modify the cluster state are invoked serially.
func (j *Job) processStateUpdates() {
	for update := range j.stateUpdates {
		update()

		if purged := j.registry.Purge(); len(purged) > 0 {
			j.log.Info("registry purged", "nodes", purged)
		}

		j.log.Info("check registry", append(
			[]any{slog.String("assembly", j.assembly.String())},
			j.registry.Diagnostics()...)...)

		if j.assembly != nil {
			if j.assembly.Healthy() {
				continue // All working nodes are good, continue
			}
			j.pause() // Need to pause because something is unhealthy
		}

		// If we're in some paused state
		if j.assembly == nil {
			// Try to create a new assembly if there are enough nodes
			assembly, err := NewAssembly(j.registry, j.config.Sources[0].NewSourceSplitter(), j.log, j.keySpace)
			if errors.Is(err, ErrNotEnoughResources) {
				continue
			}
			j.assembly = assembly
			if err := j.run(); err != nil {
				j.log.Error("failed to run the assembly", "err", err)
			}
		}
	}
}

// Transition the job to the running state.
func (j *Job) run() error {
	j.log.Info("running")

	// Get the job's current checkpoint
	ckpt, err := j.snapshotStore.LoadLatestCheckpoint()
	if err != nil {
		return fmt.Errorf("loading latest job checkpoint: %v", err)
	}

	// Load the source checkpoint into the source splitter
	if len(ckpt.GetSourceCheckpoints()) > 0 {
		ckptData := sliceu.Map(ckpt.SourceCheckpoints, func(c *snapshotpb.SourceCheckpoint) []byte {
			return c.Data
		})
		if err := j.sourceSplitter.LoadCheckpoints(ckptData); err != nil {
			return fmt.Errorf("loading checkpoint into source splitter: %v", err)
		}
	}

	// Assign splits to the source runners
	splitAssignments, err := j.sourceSplitter.AssignSplits(j.assembly.SourceRunnerIDs())
	if err != nil {
		return fmt.Errorf("assigning splits: %v", err)
	}

	// Start the assembly
	if err := j.assembly.Start(j.config, ckpt, splitAssignments); err != nil {
		return fmt.Errorf("starting assembly: %v", err)
	}

	j.checkpointTicker = j.clock.Every(1*time.Minute, func(tc *clocks.EveryContext) {
		cpID, err := j.snapshotStore.CreateCheckpoint(j.assembly.OperatorIDs(), j.assembly.SourceRunnerIDs())
		if errors.Is(err, snapshots.ErrCheckpointInProgress) {
			tc.RetryIn(1 * time.Second)
			return
		}
		if err := j.assembly.StartCheckpoint(context.Background(), cpID); err != nil {
			j.log.Error("failed to start checkpoint", "err", err)
		}
	}, "checkpointing")

	return nil
}

// Transition the job back to the init state
func (j *Job) pause() {
	j.log.Info("paused")
	j.checkpointTicker.Stop()
	j.assembly = nil
}
