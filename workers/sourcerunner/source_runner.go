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
	"reduction.dev/reduction-handler/handlerpb"
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
	host            string // A host name used to contract this SourceRunner
	ID              string
	sourceReader    connectors.SourceReader
	eoi             bool // Did the sourceReader reach end of input
	userHandler     proto.Handler
	operators       *operatorCluster
	job             proto.Job
	watermarker     *wmark.Watermarker
	watermarkTicker *time.Ticker
	clock           clocks.Clock
	isHalting       atomic.Bool
	stopLoop        context.CancelFunc      // Signal to stop the event loop if running
	stop            context.CancelCauseFunc // Signal to stop all source runner processes
	operatorFactory proto.OperatorFactory
	errChan         chan error

	// Checkpoint barriers are enqueued for processing in series with other events.
	checkpointBarrier chan *workerpb.CheckpointBarrier

	Logger         *slog.Logger
	registerPoller *clocks.Ticker
}

type NewParams struct {
	Host            string
	UserHandler     proto.Handler
	Job             proto.Job
	Clock           clocks.Clock
	OperatorFactory proto.OperatorFactory
}

func New(params NewParams) *SourceRunner {
	if params.Clock == nil {
		params.Clock = clocks.NewSystemClock()
	}

	id := ksuid.New().String()
	log := slog.With("instanceID", "source-runner-"+id[len(id)-4:])
	return &SourceRunner{
		ID:                id,
		host:              params.Host,
		job:               params.Job,
		watermarker:       &wmark.Watermarker{},
		watermarkTicker:   time.NewTicker(math.MaxInt64), // initialize with ticker that never ticks
		userHandler:       params.UserHandler,
		checkpointBarrier: make(chan *workerpb.CheckpointBarrier, 1),
		Logger:            log,
		clock:             params.Clock,
		stopLoop:          func() {},          // initialize with noop
		stop:              func(err error) {}, // initialize with noop
		operatorFactory:   params.OperatorFactory,
		errChan:           make(chan error),
	}
}

// Start begins registration attempts and is called during boot.
func (r *SourceRunner) Start(ctx context.Context) error {
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
		cancel(<-r.errChan)
	}()

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

	close(r.errChan)

	// Treat context.Canceled as a non-error case
	if cause := context.Cause(ctx); cause != context.Canceled {
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
	if len(msg.Sources) != 1 {
		panic("exactly one source required")
	}
	r.sourceReader = config.NewSourceReaderFromProto(msg.Sources[0])
	if err := r.sourceReader.SetSplits(msg.Splits); err != nil {
		return err
	}

	ops := make([]proto.Operator, len(msg.Operators))
	for i, op := range msg.Operators {
		ops[i] = r.operatorFactory(r.ID, op, make(chan error))
	}
	r.operators = newOperatorCluster(&newClusterParams{
		keyGroupCount: int(msg.KeyGroupCount),
		operators:     ops,
	})

	r.watermarkTicker = time.NewTicker(time.Millisecond * 200)

	loopCtx, cancel := context.WithCancel(context.Background())
	r.stopLoop = cancel
	go func() {
		if err := r.processEvents(loopCtx); err != nil {
			r.Logger.Error("processEvents stopped with error", "err", err)
		}
		cancel()
	}()

	return nil
}

func (r *SourceRunner) processEvents(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			r.Logger.Info("stopping source runner loop", "cause", context.Cause(ctx))
			return nil
		case <-r.watermarkTicker.C:
			if err := r.processWatermarkTick(ctx); err != nil {
				r.Logger.Error("error processing watermark tick", "err", err)
			}
		case barrier := <-r.checkpointBarrier:
			if err := r.createCheckpoint(barrier.CheckpointId); err != nil {
				return fmt.Errorf("creating checkpoint: %w", err)
			}
			if err := r.operators.broadcastEvent(ctx, barrier); err != nil {
				return fmt.Errorf("broadcast snapshot barrier event: %w", err)
			}
		default: // Read from source
			if r.eoi { // Never read source events again after reaching end of input
				continue
			}

			events, err := r.sourceReader.ReadEvents()
			if err != nil {
				if errors.Is(err, connectors.ErrEndOfInput) {
					// EOI error case may still have returned events
					for _, e := range events {
						if err := r.processEvent(ctx, e); err != nil {
							return err
						}
					}
					if err := r.processWatermarkTick(ctx); err != nil {
						return err
					}
					if err := r.operators.broadcastEvent(ctx, &workerpb.SourceCompleteEvent{}); err != nil {
						r.Logger.Error("failed broadcasting source complete", "err", err)
					}

					r.eoi = true
					continue
				}

				r.Logger.Error("failed reading source, will retry", "err", err)
				continue
			}

			for _, e := range events {
				if err := r.processEvent(ctx, e); err != nil {
					return err
				}
			}
		}
	}
}

func (r *SourceRunner) HandleStartCheckpoint(ctx context.Context, id uint64) {
	r.checkpointBarrier <- &workerpb.CheckpointBarrier{CheckpointId: id}
}

// Process one event in the event queue
func (r *SourceRunner) processEvent(ctx context.Context, event []byte) error {
	resp, err := r.userHandler.KeyEvent(ctx, &handlerpb.KeyEventBatchRequest{Values: [][]byte{event}})
	if err != nil {
		return fmt.Errorf("keying event: %v", err)
	}
	for _, ev := range resp.Events {
		r.watermarker.AdvanceTime(ev.Timestamp.AsTime())
		if err := r.operators.routeEvent(ctx, ev); err != nil {
			return err
		}
	}

	return nil
}

// processWatermarkTick broadcasts a watermark to the operators.
func (r *SourceRunner) processWatermarkTick(ctx context.Context) error {
	wm := &workerpb.Watermark{Timestamp: timestamppb.New(r.watermarker.CurrentWatermark())}
	if err := r.operators.broadcastEvent(ctx, wm); err != nil {
		return fmt.Errorf("broadcast watermark: %v", err)
	}
	return nil
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
