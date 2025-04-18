package jobs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"reduction.dev/reduction/clocks"
	"reduction.dev/reduction/config"
	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/proto"
	"reduction.dev/reduction/proto/jobpb"
	"reduction.dev/reduction/proto/snapshotpb"
	"reduction.dev/reduction/storage/locations"
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
	operatorFactory     proto.OperatorFactory
	sourceRunnerFactory proto.SourceRunnerFactory
	status              *jobStatus
}

type NewParams struct {
	JobConfig           *config.Config
	SavepointURI        string
	Clock               clocks.Clock
	HeartbeatDeadline   time.Duration
	Store               locations.StorageLocation
	CheckpointsPath     string
	SavepointsPath      string
	Logger              *slog.Logger
	OperatorFactory     proto.OperatorFactory
	SourceRunnerFactory proto.SourceRunnerFactory
	CheckpointEvents    chan snapshots.CheckpointEvent
}

func New(params *NewParams) (*Job, error) {
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
		wd, err := os.Getwd()
		if err != nil {
			return nil, fmt.Errorf("failed to get working directory: %w", err)
		}
		params.Store = locations.NewLocalDirectory(wd)
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

	retainedCheckpointsUpdatedChan := make(chan []uint64)

	snapshotStore := snapshots.NewStore(&snapshots.NewStoreParams{
		SavepointURI:               params.SavepointURI,
		FileStore:                  params.Store,
		SavepointsPath:             params.SavepointsPath,
		CheckpointsPath:            params.CheckpointsPath,
		CheckpointEvents:           params.CheckpointEvents,
		RetainedCheckpointsUpdated: retainedCheckpointsUpdatedChan,
	})
	err := snapshotStore.LoadCheckpoint()
	if err != nil {
		return nil, fmt.Errorf("failed to load initial checkpoint: %w", err)
	}

	job := &Job{
		snapshotStore:       snapshotStore,
		log:                 params.Logger,
		stateUpdates:        make(chan func()),
		registry:            NewRegistry(params.JobConfig.WorkerCount, NewLivenessTracker(params.Clock, params.HeartbeatDeadline)),
		config:              params.JobConfig,
		clock:               params.Clock,
		sourceSplitter:      params.JobConfig.Sources[0].NewSourceSplitter(),
		operatorFactory:     params.OperatorFactory,
		sourceRunnerFactory: params.SourceRunnerFactory,
		status:              newJobStatus(),
	}

	ctx := context.TODO()

	go job.processStateUpdates()

	go func() {
		for retained := range retainedCheckpointsUpdatedChan {
			job.assembly.UpdateRetainedCheckpoints(ctx, retained)
		}
	}()

	return job, nil
}

func (j *Job) HandleRegisterOperator(node *jobpb.NodeIdentity) {
	j.stateUpdates <- func() {
		j.log.Debug("registering operator", "id", node.Id, "host", node.Host)
		operator := j.operatorFactory("job", node)
		j.registry.RegisterOperator(operator)
	}
}

func (j *Job) HandleDeregisterOperator(op *jobpb.NodeIdentity) {
	j.stateUpdates <- func() {
		j.log.Debug("deregistered operator", "id", op.Id, "host", op.Host)
		j.registry.DeregisterOperator(op)
	}
}

func (j *Job) HandleRegisterSourceRunner(node *jobpb.NodeIdentity) {
	j.stateUpdates <- func() {
		j.log.Debug("registered source runner", "id", node.Id, "host", node.Host)
		sr := j.sourceRunnerFactory(node)
		j.registry.RegisterSourceRunner(sr)
	}
}

func (j *Job) HandleDeregisterSourceRunner(sr *jobpb.NodeIdentity) {
	j.stateUpdates <- func() {
		j.log.Debug("deregistered source runner", "id", sr.Id, "host", sr.Host)
		j.registry.DeregisterSourceRunner(sr)
	}
}

func (j *Job) HandleCreateSavepoint(ctx context.Context) (uint64, error) {
	if j.status.Value() != StatusRunning {
		return 0, fmt.Errorf("cannot create savepoint: job not running (status: %s)", j.status)
	}

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

func (j *Job) Close() {
	j.checkpointTicker.Stop()
	j.sourceSplitter.Close()
}

// Evaluate the cluster state and transition to paused or running. This
// method and others that modify the cluster state are invoked serially.
func (j *Job) processStateUpdates() {
	for update := range j.stateUpdates {
		// Process the state update
		update()

		// Evaluate the cluster state
		if purged := j.registry.Purge(); len(purged) > 0 {
			j.log.Info("registry purged", "nodes", purged)
		}

		if j.registry.HasChanges() {
			j.log.Info("registry updated", append(
				[]any{
					slog.String("status", j.status.String()),
					slog.String("assembly", j.assembly.String()),
				},
				j.registry.Diagnostics()...)...)
			j.registry.AcknowledgeChanges()
		}

		// Transition the job to new state if needed
		switch j.status.Value() {
		case StatusRunning:
			if ok, reason := j.assembly.Healthy(j.registry); !ok {
				j.log.Info("assembly not healthy", "reason", reason)
				j.status.Set(StatusPaused)
				if j.checkpointTicker != nil {
					j.checkpointTicker.Stop()
				}
			}

		case StatusInit, StatusPaused:
			// Try to create a new assembly if there are enough nodes
			assembly, err := j.registry.NewAssembly()
			if err != nil {
				// Not enough resources yet, continue waiting
				if errors.Is(err, ErrNotEnoughResources) {
					break
				}
				j.log.Error("failed to assemble resources", "err", err)
				break
			}

			j.status.Set(StatusAssemblyStarting)
			j.assembly = assembly

			go func() {
				err := j.start()
				if err != nil {
					j.stateUpdates <- func() {
						j.log.Error("failed to start job", "err", err)
						j.status.Set(StatusPaused)
						if j.checkpointTicker != nil {
							j.checkpointTicker.Stop()
						}
					}
				}
			}()
		}
	}
}

// Transition the job to the running state.
func (j *Job) start() error {
	j.log.Info("starting")

	// Get the job's current checkpoint which may be nil
	ckpt := j.snapshotStore.CurrentCheckpoint()

	// Load the source checkpoint into the source splitter
	if len(ckpt.GetSourceCheckpoints()) > 0 {
		ckptData := sliceu.Map(ckpt.SourceCheckpoints, func(c *snapshotpb.SourceCheckpoint) []byte {
			return c.Data
		})
		if err := j.sourceSplitter.LoadCheckpoints(ckptData); err != nil {
			return fmt.Errorf("loading checkpoint into source splitter: %v", err)
		}
	}

	// Allow the source splitter to start any background work
	j.sourceSplitter.Start()

	// Assign splits to the source runners
	splitAssignments, err := j.sourceSplitter.AssignSplits(j.assembly.SourceRunnerIDs())
	if err != nil {
		return fmt.Errorf("assigning splits: %v", err)
	}

	// Start the assembly
	if err := j.assembly.Start(j.config, ckpt, splitAssignments); err != nil {
		return fmt.Errorf("starting assembly: %v", err)
	}

	j.stateUpdates <- func() {
		j.log.Info("running")
		j.status.Set(StatusRunning)

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
	}

	return nil
}
