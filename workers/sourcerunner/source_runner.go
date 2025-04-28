package sourcerunner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sync/atomic"
	"time"

	"github.com/segmentio/ksuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	"reduction.dev/reduction-protocol/handlerpb"
	"reduction.dev/reduction-protocol/jobconfigpb"
	"reduction.dev/reduction/batching"
	"reduction.dev/reduction/clocks"
	"reduction.dev/reduction/config"
	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/proto"
	"reduction.dev/reduction/proto/jobpb"
	"reduction.dev/reduction/proto/workerpb"
	"reduction.dev/reduction/workers/wmark"
)

type SourceRunner struct {
	host                string // A host name used to contract this SourceRunner
	ID                  string
	sourceReader        connectors.SourceReader
	userHandler         proto.Handler
	operators           *operatorCluster
	job                 proto.Job
	watermarker         *wmark.Watermarker
	watermarkTicker     *time.Ticker
	clock               clocks.Clock
	isHalting           atomic.Bool
	stopLoop            context.CancelFunc      // Signal to stop the event loop if running
	stop                context.CancelCauseFunc // Signal to stop all source runner processes
	operatorFactory     proto.OperatorFactory
	sourceReaderFactory func(*jobconfigpb.Source) connectors.SourceReader
	errChan             chan error
	batchingParams      batching.EventBatcherParams
	outputStream        chan *workerpb.Event
	keyEventChannel     *batching.ReorderFetcher[[]byte, []*handlerpb.KeyedEvent]
	initDone            chan struct{}                 // Used by HandleStart to wait until Start finishes initializing
	sourceChannel       *connectors.ReadSourceChannel // The channel to read events from the source

	// Checkpoint barriers are enqueued for processing in series with other events.
	checkpointBarrier chan *workerpb.CheckpointBarrier

	Logger         *slog.Logger
	registerPoller *clocks.Ticker

	// Add a context field to SourceRunner for use in HandleAssignSplits
	ctx context.Context

	runnerCtx context.Context // Context tied to SourceRunner lifecycle

	startSourceChannelCh chan struct{} // signal to start the source channel

	splitsWereAssigned chan []*workerpb.SourceSplit // channel for split assignment requests
}

type NewParams struct {
	Host                string
	UserHandler         proto.Handler
	Job                 proto.Job
	Clock               clocks.Clock
	OperatorFactory     proto.OperatorFactory
	SourceReaderFactory func(*jobconfigpb.Source) connectors.SourceReader
	EventBatching       batching.EventBatcherParams
}

func New(params NewParams) *SourceRunner {
	if params.Clock == nil {
		params.Clock = clocks.NewSystemClock()
	}

	srID := ksuid.New().String()

	log := slog.With("instanceID", "source-runner-"+srID[len(srID)-4:])
	sr := &SourceRunner{
		ID:                  srID,
		host:                params.Host,
		job:                 params.Job,
		watermarker:         &wmark.Watermarker{},
		watermarkTicker:     time.NewTicker(math.MaxInt64), // initialize with ticker that never ticks
		userHandler:         params.UserHandler,
		checkpointBarrier:   make(chan *workerpb.CheckpointBarrier, 1),
		Logger:              log,
		clock:               params.Clock,
		stopLoop:            func() {},          // initialize with noop
		stop:                func(err error) {}, // initialize with noop
		operatorFactory:     params.OperatorFactory,
		sourceReaderFactory: params.SourceReaderFactory,
		errChan:             make(chan error),
		batchingParams:      params.EventBatching,
		outputStream:        make(chan *workerpb.Event, 1_000),
		initDone:            make(chan struct{}),
		splitsWereAssigned:  make(chan []*workerpb.SourceSplit, 1),
	}

	if sr.sourceReaderFactory == nil {
		sr.sourceReaderFactory = func(source *jobconfigpb.Source) connectors.SourceReader {
			sourceConfig, err := config.SourceFromProto(source)
			if err != nil {
				panic(fmt.Sprintf("invalid source config: %v", err))
			}
			return sourceConfig.NewSourceReader(connectors.SourceReaderHooks{
				NotifySplitsFinished: func(splitIDs []string) {
					sr.job.NotifySplitsFinished(sr.runnerCtx, sr.ID, splitIDs)
				},
			})
		}
	}

	return sr
}

// Start begins registration attempts and is called during boot.
func (r *SourceRunner) Start(ctx context.Context) error {
	r.Logger.Info("starting")
	defer r.Logger.Info("stopped event loop")

	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	r.stop = cancel
	r.runnerCtx = ctx // Store the lifecycle context

	r.registerPoller = r.clock.Every(3*time.Second, func(*clocks.EveryContext) {
		if err := r.job.RegisterSourceRunner(ctx, &jobpb.NodeIdentity{
			Id:   r.ID,
			Host: r.host,
		}); err != nil {
			r.Logger.Error("registering source runner with job service failed", "err", err)
		}
	}, "register")
	r.registerPoller.Trigger() // Try to register immediately

	// Listen for errors on the errChan
	go func() {
		result := <-r.errChan
		r.Logger.Error("source runner error", "err", result)
		cancel(result)
	}()

	r.keyEventChannel = batching.NewReorderFetcher(ctx, batching.NewReorderFetcherParams[[]byte, []*handlerpb.KeyedEvent]{
		Batcher: batching.NewEventBatcher[[]byte](ctx, r.batchingParams),
		FetchBatch: func(ctx context.Context, events [][]byte) ([][]*handlerpb.KeyedEvent, error) {
			return r.userHandler.KeyEventBatch(ctx, events)
		},
		ErrChan:    r.errChan,
		BufferSize: r.batchingParams.MaxSize,
	})

	close(r.initDone) // Signal that initialization is done

	<-ctx.Done() // Afterward begin shutdown process

	r.Logger.Info("starting shutdown")
	defer r.Logger.Info("shutdown complete")

	r.stopLoop()
	r.watermarkTicker.Stop()
	r.registerPoller.Stop()

	if !r.isHalting.Load() {
		if err := r.job.DeregisterSourceRunner(context.Background(), &jobpb.NodeIdentity{Id: r.ID, Host: r.host}); err != nil {
			r.Logger.Warn("failed deregistration", "err", err)
		}
	}

	// Treat context.Canceled as a non-error case
	if cause := context.Cause(ctx); !errors.Is(cause, context.Canceled) {
		return cause
	}
	return nil
}

// Stop signals the SR to shutdown. This is safe to call multiple times.
func (r *SourceRunner) Stop() error {
	r.stop(nil)
	return nil
}

// Halt stops the source runner without deregistering from the job
func (r *SourceRunner) Halt() {
	r.isHalting.Store(true)
	r.stop(nil)
}

// HandleDeploy is invoked by the Job when the SourceRunner has joined an assembly.
func (r *SourceRunner) HandleDeploy(ctx context.Context, msg *workerpb.DeploySourceRunnerRequest) error {
	<-r.initDone // Wait for Start to finish initializing

	if len(msg.Sources) != 1 {
		panic("exactly one source required")
	}

	r.watermarkTicker = time.NewTicker(time.Millisecond * 200)

	deploymentCtx, cancel := context.WithCancel(context.Background())
	r.stopLoop = cancel
	r.ctx = deploymentCtx // assign loopCtx to r.ctx for later use

	r.sourceReader = r.sourceReaderFactory(msg.Sources[0])
	r.sourceChannel = connectors.NewReadSourceChannel(r.sourceReader)

	ops := make([]proto.Operator, len(msg.Operators))
	for i, op := range msg.Operators {
		ops[i] = r.operatorFactory(r.ID, op)
	}
	r.operators = newOperatorCluster(r.ctx, &newClusterParams{
		keyGroupCount:  int(msg.KeyGroupCount),
		operators:      ops,
		batchingParams: r.batchingParams,
		errChan:        r.errChan,
	})

	go func() {
		if err := r.processEvents(r.ctx); err != nil {
			r.Logger.Error("processEvents stopped with error", "err", err)
		}
		cancel()
	}()
	go func() {
		for opEvent := range r.outputStream {
			if err := r.sendOperatorEvent(opEvent); err != nil {
				r.errChan <- err
			}
		}
	}()

	return nil
}

func (r *SourceRunner) processEvents(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			r.Logger.Info("stopping source runner loop", "cause", context.Cause(ctx))
			return nil
		case splits := <-r.splitsWereAssigned:
			if err := r.sourceReader.AssignSplits(splits); err != nil {
				return err
			}
			if len(splits) > 0 {
				if r.sourceChannel == nil {
					return fmt.Errorf("sourceChannel is nil")
				}
				r.sourceChannel.Start(r.ctx)
			}
		case <-r.watermarkTicker.C:
			r.outputStream <- &workerpb.Event{Event: &workerpb.Event_Watermark{Watermark: &workerpb.Watermark{}}}
		case barrier := <-r.checkpointBarrier:
			if err := r.createCheckpoint(barrier.CheckpointId); err != nil {
				return fmt.Errorf("creating checkpoint: %w", err)
			}
			r.outputStream <- &workerpb.Event{Event: &workerpb.Event_CheckpointBarrier{CheckpointBarrier: barrier}}
		case readFunc, ok := <-r.sourceChannel.C:
			if !ok {
				// Channel closed, Flush events, Send watermark, send source complete event
				r.keyEventChannel.Flush(ctx)
				r.outputStream <- &workerpb.Event{Event: &workerpb.Event_Watermark{
					Watermark: &workerpb.Watermark{},
				}}
				r.outputStream <- &workerpb.Event{Event: &workerpb.Event_SourceComplete{}}

				// Stop reading from sourceChannel
				r.sourceChannel = nil
				continue
			}

			// Execute the read function to get events
			events, err := readFunc()
			if err != nil {
				if connectors.IsRetryable(err) {
					r.Logger.Error("failed reading source, will retry", "err", err)
					continue
				}

				return fmt.Errorf("terminal source reader error: %w", err)
			}

			// Process the events
			for _, e := range events {
				r.sendKeyEvent(ctx, e)
			}
		}
	}
}

func (r *SourceRunner) HandleStartCheckpoint(ctx context.Context, id uint64) {
	r.checkpointBarrier <- &workerpb.CheckpointBarrier{CheckpointId: id}
}

func (r *SourceRunner) sendOperatorEvent(event *workerpb.Event) error {
	switch typedEvent := event.Event.(type) {
	case *workerpb.Event_KeyedEvent:
		// Get the async result for this placeholder event
		asyncResult := <-r.keyEventChannel.Output
		for _, event := range asyncResult {
			r.watermarker.AdvanceTime(event.Timestamp.AsTime())
			r.operators.routeEvent(event.Key, &workerpb.Event{
				Event: &workerpb.Event_KeyedEvent{
					KeyedEvent: event,
				},
			})
		}
		return nil
	case *workerpb.Event_Watermark:
		typedEvent.Watermark.Timestamp = timestamppb.New(r.watermarker.CurrentWatermark())
		return r.operators.broadcastEvent(typedEvent.Watermark)
	case *workerpb.Event_CheckpointBarrier:
		return r.operators.broadcastEvent(typedEvent.CheckpointBarrier)
	case *workerpb.Event_SourceComplete:
		if err := r.operators.broadcastEvent(typedEvent.SourceComplete); err != nil {
			return err
		}
		r.operators.flush()
		return nil
	default:
		return fmt.Errorf("unknown operator event type: %T", typedEvent)
	}
}

func (r *SourceRunner) sendKeyEvent(ctx context.Context, event []byte) {
	// Put a placeholder on the output stream that will be joined with the async
	// KeyEvent result.
	r.outputStream <- &workerpb.Event{Event: &workerpb.Event_KeyedEvent{}}
	r.keyEventChannel.Add(ctx, event)
}

// createCheckpoint gets checkpoint data from the source reader and notifies the job.
func (r *SourceRunner) createCheckpoint(id uint64) error {
	data := r.sourceReader.Checkpoint()
	return r.job.OnSourceRunnerCheckpointComplete(context.Background(), &jobpb.SourceRunnerCheckpointCompleteRequest{
		CheckpointId:   id,
		SplitStates:    data,
		SourceRunnerId: r.ID,
	})
}

// HandleAssignSplits is invoked to assign new splits and start reading from them.
func (r *SourceRunner) HandleAssignSplits(splits []*workerpb.SourceSplit) error {
	if r.sourceReader == nil {
		return fmt.Errorf("sourceReader is not initialized")
	}
	r.splitsWereAssigned <- splits
	return nil
}
