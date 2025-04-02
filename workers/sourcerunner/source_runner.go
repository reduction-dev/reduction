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
	"reduction.dev/reduction/proto/snapshotpb"
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
	initDone            chan struct{} // Used by HandleStart to wait until Start finishes initializing

	// Checkpoint barriers are enqueued for processing in series with other events.
	checkpointBarrier chan *workerpb.CheckpointBarrier

	Logger         *slog.Logger
	registerPoller *clocks.Ticker
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

	if params.SourceReaderFactory == nil {
		params.SourceReaderFactory = func(source *jobconfigpb.Source) connectors.SourceReader {
			sourceConfig, err := config.SourceFromProto(source)
			if err != nil {
				panic(fmt.Sprintf("invalid source config: %v", err))
			}
			return sourceConfig.NewSourceReader()
		}
	}

	id := ksuid.New().String()
	log := slog.With("instanceID", "source-runner-"+id[len(id)-4:])
	return &SourceRunner{
		ID:                  id,
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
	}
}

// Start begins registration attempts and is called during boot.
func (r *SourceRunner) Start(ctx context.Context) error {
	r.Logger.Info("starting")
	defer r.Logger.Info("stopped event loop")

	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	r.stop = cancel

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

// HandleStart is invoked by the Job when the SourceRunner has joined an assembly.
func (r *SourceRunner) HandleStart(ctx context.Context, msg *workerpb.StartSourceRunnerRequest) error {
	<-r.initDone // Wait for Start to finish initializing

	if len(msg.Sources) != 1 {
		panic("exactly one source required")
	}

	r.watermarkTicker = time.NewTicker(time.Millisecond * 200)

	r.sourceReader = r.sourceReaderFactory(msg.Sources[0])
	if err := r.sourceReader.SetSplits(msg.Splits); err != nil {
		return err
	}

	loopCtx, cancel := context.WithCancel(context.Background())

	ops := make([]proto.Operator, len(msg.Operators))
	for i, op := range msg.Operators {
		ops[i] = r.operatorFactory(r.ID, op)
	}
	r.operators = newOperatorCluster(loopCtx, &newClusterParams{
		keyGroupCount:  int(msg.KeyGroupCount),
		operators:      ops,
		batchingParams: r.batchingParams,
		errChan:        r.errChan,
	})

	r.stopLoop = cancel
	go func() {
		if err := r.processEvents(loopCtx); err != nil {
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
	readEvents := connectors.NewReadSourceChannel(ctx, r.sourceReader)

	for {
		select {
		case <-ctx.Done():
			r.Logger.Info("stopping source runner loop", "cause", context.Cause(ctx))
			return nil
		case <-r.watermarkTicker.C:
			r.outputStream <- &workerpb.Event{Event: &workerpb.Event_Watermark{Watermark: &workerpb.Watermark{}}}
		case barrier := <-r.checkpointBarrier:
			if err := r.createCheckpoint(barrier.CheckpointId); err != nil {
				return fmt.Errorf("creating checkpoint: %w", err)
			}
			r.outputStream <- &workerpb.Event{Event: &workerpb.Event_CheckpointBarrier{CheckpointBarrier: barrier}}
		case readFunc, ok := <-readEvents:
			if !ok {
				// Channel closed, Flush events, Send watermark, send source complete event
				r.keyEventChannel.Flush(ctx)
				r.outputStream <- &workerpb.Event{Event: &workerpb.Event_Watermark{
					Watermark: &workerpb.Watermark{},
				}}
				r.outputStream <- &workerpb.Event{Event: &workerpb.Event_SourceComplete{}}

				// Stop reading from readEvents
				readEvents = nil
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
	return r.job.OnSourceCheckpointComplete(context.Background(), &snapshotpb.SourceCheckpoint{
		CheckpointId:   id,
		Data:           data,
		SourceRunnerId: r.ID,
	})
}
