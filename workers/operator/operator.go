package operator

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/ksuid"
	"google.golang.org/protobuf/types/known/timestamppb"
	"reduction.dev/reduction-handler/handlerpb"
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
	id                  string
	host                string
	job                 proto.Job
	userHandler         proto.Handler
	Logger              *slog.Logger
	mu                  *sync.RWMutex
	clock               clocks.Clock
	events              chan func()
	stop                context.CancelFunc
	isHalting           atomic.Bool
	isInAssembly        atomic.Bool
	eventBatcher        *batching.EventBatcher[RxnHandlerEvent]
	eventBatchingParams batching.EventBatcherParams

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
	ID            string // Optional ID to override ID generation
	Host          string
	Job           proto.Job
	UserHandler   proto.Handler
	EventBatching batching.EventBatcherParams
	Clock         clocks.Clock
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
		id:                  params.ID,
		host:                params.Host,
		job:                 params.Job,
		userHandler:         params.UserHandler,
		Logger:              log,
		mu:                  &sync.RWMutex{},
		clock:               params.Clock,
		events:              make(chan func()),
		stop:                func() {}, // initialize with noop
		eventBatchingParams: params.EventBatching,
	}
}

func (o *Operator) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	o.stop = cancel

	o.registerPoller = o.clock.Every(3*time.Second, func(ec *clocks.EveryContext) {
		o.mu.RLock()
		defer o.mu.RUnlock()

		if err := o.job.RegisterOperator(ctx, &jobpb.NodeIdentity{
			Id:   o.id,
			Host: o.host,
		}); err != nil {
			o.Logger.Error("failed to register with job, will retry", "err", err)
		}
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

func (o *Operator) HandleStart(ctx context.Context, req *workerpb.StartOperatorRequest, sink connectors.SinkWriter) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	// Locate own index by in the list of provided operator IDs
	assemblyIndex := slices.IndexFunc(req.OperatorIds, func(id string) bool {
		return id == o.id
	})
	if assemblyIndex == -1 {
		panic(fmt.Sprintf("operator id %s was not included in assembly ids provided by job, %v", o.id, req.OperatorIds))
	}

	// Instantiate key space members
	o.keySpace = partitioning.NewKeySpace(int(req.KeyGroupCount), len(req.OperatorIds))
	o.keyGroupRange = o.keySpace.KeyGroupRanges()[assemblyIndex]

	// Start the DKV database.
	ckptHandles := make([]recovery.CheckpointHandle, len(req.Checkpoints))
	for i, ckpt := range req.Checkpoints {
		ckptHandles[i] = recovery.CheckpointHandle{
			CheckpointID: ckpt.CheckpointId,
			URI:          ckpt.DkvFileUri,
		}
	}
	o.db = dkv.Open(dkv.DBOptions{
		FileSystem: storage.NewFileSystemFromLocation(filepath.Join(req.StorageLocation, o.id)),
	}, ckptHandles)
	o.stateStore = NewKeyedStateStore(o.db, o.keySpace)
	o.timerRegistry = NewTimerRegistry(NewTimerStore(o.db, o.keySpace, o.keyGroupRange, size.GB), req.SourceRunnerIds)

	// Set upstream source runner IDs
	o.sourceRunners = newUpstreams(req.SourceRunnerIds)

	o.sink = sink

	o.Logger.Info("start command", "operatorIDs", req.OperatorIds, "keyGroupRange", o.keyGroupRange)

	o.isInAssembly.Store(true)

	return nil
}

func (o *Operator) HandleEvent(ctx context.Context, senderID string, req *workerpb.Event) error {
	if !o.isInAssembly.Load() {
		return fmt.Errorf("not running")
	}

	o.Logger.Info("handling", "sender", senderID, "event", req.Event)

	// Collect the err response from the queued event.
	respErr := make(chan error)

	// Block the sender to align checkpoint barriers if needed
	o.mu.RLock()
	waitOnAlignment := o.checkpoint.alignSender(senderID)
	o.mu.RUnlock()
	waitOnAlignment()

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

	// First collect all keyStates and events
	keyStates := make([]*handlerpb.KeyState, len(batchEvents))
	events := make([]*handlerpb.Event, len(batchEvents))
	for i, rxnEvent := range batchEvents {
		// State
		state, err := o.stateStore.GetState(rxnEvent.key)
		if err != nil {
			return fmt.Errorf("getting state for processEventBatch: %v", err)
		}
		keyStates[i] = &handlerpb.KeyState{
			Key:                  rxnEvent.key,
			StateEntryNamespaces: state,
		}

		// Event
		events[i] = rxnEvent.event
	}

	// Make a single batch call to the handler
	resp, err := o.userHandler.ProcessEventBatch(ctx, &handlerpb.ProcessEventBatchRequest{
		KeyStates: keyStates,
		Events:    events,
	})
	if err != nil {
		return err
	}

	// Process results
	for _, keyResult := range resp.KeyResults {
		// Register any new timers
		for _, t := range keyResult.NewTimers {
			o.Logger.Info("registering timer", "timer", t)
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
