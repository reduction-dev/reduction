package operator

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	"github.com/segmentio/ksuid"
	"google.golang.org/protobuf/types/known/timestamppb"
	"reduction.dev/reduction-protocol/handlerpb"
	"reduction.dev/reduction/batching"
	"reduction.dev/reduction/clocks"
	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/dkv"
	"reduction.dev/reduction/dkv/recovery"
	"reduction.dev/reduction/dkv/storage"
	"reduction.dev/reduction/partitioning"
	"reduction.dev/reduction/proto"
	"reduction.dev/reduction/proto/jobpb"
	"reduction.dev/reduction/proto/snapshotpb"
	"reduction.dev/reduction/proto/workerpb"
	"reduction.dev/reduction/util/size"
)

type Operator struct {
	// Members initialized with NewOperator
	id                      string
	host                    string
	job                     proto.Job
	userHandler             proto.Handler
	Logger                  *slog.Logger
	mu                      *sync.RWMutex
	clock                   clocks.Clock
	events                  chan func()
	stop                    context.CancelFunc
	isHalting               atomic.Bool
	status                  *operatorStatus
	eventBatcher            *batching.EventBatcher[RxnHandlerEvent]
	eventBatchingParams     batching.EventBatcherParams
	neighborOperatorFactory func(senderID string, node *jobpb.NodeIdentity) proto.Operator

	// Members set in HandleStart
	keySpace       *partitioning.KeySpace
	sink           connectors.SinkWriter
	registerPoller *clocks.Ticker
	keyGroupRange  partitioning.KeyGroupRange
	db             *dkv.DB
	stateStore     *KeyedStateStore
	timerRegistry  *TimerRegistry
	sourceRunners  *upstreams

	checkpoint *checkpoint
}

// Events bound for the user's handler
type RxnHandlerEvent struct {
	key   []byte
	event *handlerpb.Event
}

type NewOperatorParams struct {
	ID                      string // Optional ID to override ID generation
	Host                    string
	Job                     proto.Job
	UserHandler             proto.Handler
	EventBatching           batching.EventBatcherParams
	Clock                   clocks.Clock
	NeighborOperatorFactory func(senderID string, node *jobpb.NodeIdentity) proto.Operator
}

func NewOperator(params NewOperatorParams) *Operator {
	if params.ID == "" {
		params.ID = ksuid.New().String()
	}

	if params.Clock == nil {
		params.Clock = clocks.NewSystemClock()
	}

	shortID := params.ID
	if len(params.ID) > 4 {
		shortID = params.ID[len(params.ID)-4:]
	}
	log := slog.With("instanceID", "operator-"+shortID)
	return &Operator{
		id:                      params.ID,
		host:                    params.Host,
		job:                     params.Job,
		userHandler:             params.UserHandler,
		Logger:                  log,
		mu:                      &sync.RWMutex{},
		clock:                   params.Clock,
		events:                  make(chan func()),
		stop:                    func() {}, // initialize with noop
		eventBatchingParams:     params.EventBatching,
		status:                  newOperatorStatus(),
		neighborOperatorFactory: params.NeighborOperatorFactory,
	}
}

func (o *Operator) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	o.stop = cancel

	o.registerPoller = o.clock.Every(3*time.Second, func(ec *clocks.EveryContext) {
		if err := o.job.RegisterOperator(ctx, &jobpb.NodeIdentity{
			Id:   o.id,
			Host: o.host,
		}); err != nil {
			o.Logger.Error("failed to register with job, will retry", "err", err)
		}
		o.status.DidRegister()
	}, "register")
	o.registerPoller.Trigger() // Try to register immediately

	o.eventBatcher = batching.NewEventBatcher[RxnHandlerEvent](ctx, o.eventBatchingParams)

	go func() {
		if err := o.processEvents(ctx); err != nil {
			o.Logger.Error("processEvents failed", "err", err)
		}
		cancel()
	}()

	<-ctx.Done() // Afterward, begin shutdown

	o.Logger.Info("stopping")
	if err := o.db.Close(); err != nil {
		return err
	}
	if o.registerPoller != nil {
		o.registerPoller.Stop()
	}
	if !o.isHalting.Load() {
		o.job.DeregisterOperator(context.Background(), &jobpb.NodeIdentity{Id: o.id, Host: o.host})
	}
	o.Logger.Info("stopped")
	return nil
}

// Only called externally
func (o *Operator) Stop() error {
	o.stop()
	return nil
}

func (o *Operator) Halt() {
	o.isHalting.Store(true)
	o.stop()
}

func (o *Operator) HandleDeploy(ctx context.Context, req *workerpb.DeployOperatorRequest, sink connectors.SinkWriter) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.Logger.Info("deploy command",
		"operators", req.Operators,
		"keyGroupCount", req.KeyGroupCount,
		"checkpoints", req.Checkpoints)

	// Locate own index by in the list of provided operator IDs
	ownIndex := slices.IndexFunc(req.Operators, func(op *jobpb.NodeIdentity) bool {
		return op.Id == o.id
	})
	if ownIndex == -1 {
		panic(fmt.Sprintf("operator id %s was not included in assembly ids provided by job, %v", o.id, req.Operators))
	}

	// Instantiate key space members
	o.keySpace = partitioning.NewKeySpace(int(req.KeyGroupCount), len(req.Operators))
	keyGroupRanges := o.keySpace.KeyGroupRanges()
	o.keyGroupRange = keyGroupRanges[ownIndex]

	// Create the neighbor operator partitions
	neighborPartitions := make([]neighborPartition, 0, len(req.Operators)-1)
	for i, op := range req.Operators {
		if i == ownIndex {
			continue
		}
		neighborPartitions = append(neighborPartitions, neighborPartition{
			keyGroupRange: keyGroupRanges[i],
			operator:      o.neighborOperatorFactory(o.id, op),
		})
	}

	ckptHandles := make([]recovery.CheckpointHandle, len(req.Checkpoints))
	for i, ckpt := range req.Checkpoints {
		ckptHandles[i] = recovery.CheckpointHandle{
			CheckpointID: ckpt.CheckpointId,
			URI:          ckpt.DkvFileUri,
		}
	}

	// Update status to loading before opening database
	o.status.LoadingStarted()
	dkvOpenStart := time.Now()

	// Initialize the DKV filesystem using the ID for a prefix
	fs, err := storage.NewFileSystemFromLocation(storage.Join(req.StorageLocation, o.id))
	if err != nil {
		return fmt.Errorf("creating filesystem: %w", err)
	}

	// Start the DKV database.
	o.db = dkv.Open(dkv.DBOptions{
		FileSystem:    fs,
		Logger:        o.Logger,
		DataOwnership: newOperatorPartition(o.keyGroupRange, neighborPartitions),
	}, ckptHandles)
	o.stateStore = NewKeyedStateStore(o.db, o.keySpace)
	o.timerRegistry = NewTimerRegistry(NewTimerStore(o.db, o.keySpace, o.keyGroupRange, size.GB), req.SourceRunnerIds)

	o.sourceRunners = newUpstreams(req.SourceRunnerIds)
	o.sink = sink

	if err := o.status.DidLoad(); err != nil {
		return fmt.Errorf("invalid status transition: %w", err)
	}
	o.Logger.Info("operator ready", "dkvLoadTime", time.Since(dkvOpenStart))

	return nil
}

func (o *Operator) HandleRemoveCheckpoints(ctx context.Context, req *workerpb.UpdateRetainedCheckpointsRequest) error {
	return o.db.UpdateRetainedCheckpoints(req.CheckpointIds)
}

func (o *Operator) HandleNeedsTable(fileURI string) bool {
	return o.db.NeedsTable(fileURI)
}

func (o *Operator) HandleEvent(ctx context.Context, senderID string, req *workerpb.Event) error {
	if !o.status.IsReady() {
		err := fmt.Errorf("operator not ready to handle events (status: %s)", o.status)
		o.Logger.Debug("HandleEvent", "sender", senderID, "event", req.Event, "status", o.status, "err", err)
		return connect.NewError(connect.CodeUnavailable, err)
	}

	o.Logger.Debug("handling", "sender", senderID, "event", req.Event)

	// Block the sender to align checkpoint barriers if needed
	o.mu.RLock()
	waitOnAlignment := o.checkpoint.alignSender(senderID)
	o.mu.RUnlock()
	waitOnAlignment()

	// Collect the err response from the queued event.
	respErr := make(chan error)
	switch typedEvent := req.Event.(type) {
	case *workerpb.Event_KeyedEvent:
		o.events <- func() {
			respErr <- o.handleUserEvent(ctx, typedEvent.KeyedEvent)
		}
	case *workerpb.Event_Watermark:
		o.events <- func() {
			respErr <- o.handleWatermark(ctx, senderID, typedEvent.Watermark)
		}
	case *workerpb.Event_CheckpointBarrier:
		o.events <- func() {
			respErr <- o.handleCheckpointBarrier(ctx, senderID, typedEvent.CheckpointBarrier)
		}
	case *workerpb.Event_SourceComplete:
		o.events <- func() {
			respErr <- o.handleSourceComplete(senderID)
		}
	default:
		return fmt.Errorf("unknown event to handle %v", typedEvent)
	}

	return <-respErr
}

// Process events until the o.events channel is closed or the context is canceled.
func (o *Operator) processEvents(ctx context.Context) error {
	for {
		select {
		case event, ok := <-o.events:
			if !ok {
				return nil
			}
			event()
		case batchToken, ok := <-o.eventBatcher.BatchTimedOut:
			if !ok {
				return nil
			}
			if err := o.processEventBatch(ctx, batchToken); err != nil {
				return err
			}
		case <-ctx.Done():
			return nil
		}
	}
}

func (o *Operator) handleUserEvent(ctx context.Context, event *handlerpb.KeyedEvent) error {
	o.eventBatcher.Add(RxnHandlerEvent{
		key:   event.Key,
		event: &handlerpb.Event{Event: &handlerpb.Event_KeyedEvent{KeyedEvent: event}},
	})
	if o.eventBatcher.IsFull() {
		return o.processEventBatch(ctx, batching.CurrentBatch)
	}
	return nil
}

func (o *Operator) handleWatermark(ctx context.Context, senderID string, wm *workerpb.Watermark) error {
	for key, t := range o.timerRegistry.AdvanceWatermark(senderID, wm) {
		o.eventBatcher.Add(RxnHandlerEvent{
			key: key,
			event: &handlerpb.Event{Event: &handlerpb.Event_TimerExpired{
				TimerExpired: &handlerpb.TimerExpired{
					Key:       key,
					Timestamp: timestamppb.New(t),
				},
			}},
		})
		if o.eventBatcher.IsFull() {
			if err := o.processEventBatch(ctx, batching.CurrentBatch); err != nil {
				return err
			}
		}
	}

	return nil
}

func (o *Operator) handleCheckpointBarrier(ctx context.Context, senderID string, barrier *workerpb.CheckpointBarrier) error {
	// Get lock for checkpoint resource which is shared between all callers.
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.checkpoint == nil {
		o.checkpoint = newCheckpoint(barrier.CheckpointId, o.sourceRunners.all)
	}
	if err := o.checkpoint.registerBarrier(senderID, barrier); err != nil {
		return err
	}

	if o.checkpoint.hasAllBarriers() {
		o.processEventBatch(ctx, batching.CurrentBatch) // Must flush any pending events before checkpointing
		cp, err := o.db.Checkpoint(o.checkpoint.checkpointID)()
		if err != nil {
			return err
		}

		err = o.job.OperatorCheckpointComplete(ctx, &snapshotpb.OperatorCheckpoint{
			CheckpointId: o.checkpoint.checkpointID,
			OperatorId:   o.id,
			DkvFileUri:   cp.URI,
			KeyGroupRange: &snapshotpb.KeyGroupRange{
				Start: int32(o.keyGroupRange.Start),
				End:   int32(o.keyGroupRange.End),
			},
		})
		if err != nil {
			return err
		}

		// Reset checkpoint to nil for next checkpoint
		o.checkpoint = nil
		return nil
	}

	return nil
}

func (o *Operator) handleSourceComplete(sourceRunnerID string) error {
	o.Logger.Info("got source complete event", "sourceRunnerID", sourceRunnerID)
	if err := o.processEventBatch(context.Background(), batching.CurrentBatch); err != nil {
		return err
	}
	o.sourceRunners.deactivate(sourceRunnerID)
	if !o.sourceRunners.hasActive() {
		o.stop()
	}
	return nil
}

func (o *Operator) processEventBatch(ctx context.Context, batchToken batching.BatchToken) error {
	batchEvents := o.eventBatcher.Flush(batchToken)
	if len(batchEvents) == 0 {
		return nil
	}

	// Create a map to cache KeyStates by key in this batch
	keyStateMap := make(map[string]*handlerpb.KeyState)
	events := make([]*handlerpb.Event, len(batchEvents))

	// First collect all keyStates and events
	for i, rxnEvent := range batchEvents {
		// Collect the Event
		events[i] = rxnEvent.event

		// Collect the State if we haven't seen this key before
		keyStr := string(rxnEvent.key)
		if _, ok := keyStateMap[keyStr]; !ok {
			state, err := o.stateStore.GetState(rxnEvent.key)
			if err != nil {
				return fmt.Errorf("getting state for processEventBatch: %v", err)
			}
			keyStateMap[keyStr] = &handlerpb.KeyState{
				Key:                  rxnEvent.key,
				StateEntryNamespaces: state,
			}
		}
	}

	// Convert map values to slice
	keyStates := make([]*handlerpb.KeyState, 0, len(keyStateMap))
	for _, ks := range keyStateMap {
		keyStates = append(keyStates, ks)
	}

	// Make a single batch call to the handler
	resp, err := o.userHandler.ProcessEventBatch(ctx, &handlerpb.ProcessEventBatchRequest{
		KeyStates: keyStates,
		Events:    events,
		Watermark: timestamppb.New(o.timerRegistry.watermark),
	})
	if err != nil {
		return err
	}

	// Process results
	for _, keyResult := range resp.KeyResults {
		// Register any new timers
		for _, t := range keyResult.NewTimers {
			o.Logger.Debug("registering timer", "timer", t)
			o.timerRegistry.SetTimer(keyResult.Key, t.AsTime())
		}
		// Apply state mutations
		if err := o.stateStore.ApplyMutations(keyResult.Key, keyResult.StateMutationNamespaces); err != nil {
			return err
		}
	}

	// Send all sink requests
	for _, r := range resp.SinkRequests {
		if err := o.sink.Write(r.Value); err != nil {
			return err
		}
	}

	return nil
}
