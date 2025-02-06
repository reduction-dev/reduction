package workers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"golang.org/x/sync/errgroup"
	"reduction.dev/reduction/batching"
	"reduction.dev/reduction/clocks"
	"reduction.dev/reduction/proto"
	"reduction.dev/reduction/workers/operator"
	"reduction.dev/reduction/workers/sourcerunner"
)

type NewParams struct {
	Host            string
	Handler         proto.Handler
	Job             proto.Job
	Diagnostics     []any // Debug arguments logged when starting the worker
	Clock           clocks.Clock
	LogPrefix       string // Optional log value to differentiate worker logs
	OperatorFactory proto.OperatorFactory
	EventBatching   batching.EventBatcherParams
}

func New(params NewParams) *Worker {
	sourceRunner := sourcerunner.New(sourcerunner.NewParams{
		Host:            params.Host,
		UserHandler:     params.Handler,
		Job:             params.Job,
		OperatorFactory: params.OperatorFactory,
		EventBatching:   params.EventBatching,
		Clock:           params.Clock,
	})

	operator := operator.NewOperator(operator.NewOperatorParams{
		Host:          params.Host,
		Job:           params.Job,
		UserHandler:   params.Handler,
		Clock:         params.Clock,
		EventBatching: params.EventBatching,
	})

	if params.LogPrefix == "" {
		params.LogPrefix = "worker"
	}

	return &Worker{
		SourceRunner: sourceRunner,
		Operator:     operator,
		job:          params.Job,
		diagnostics:  params.Diagnostics,
		log:          slog.With("instanceID", params.LogPrefix),
	}
}

// Worker runs a SourceRunner and Operator instance.
type Worker struct {
	// The processes running on this worker
	SourceRunner *sourcerunner.SourceRunner
	Operator     *operator.Operator

	job         proto.Job // Client for the job manager
	diagnostics []any     // List of arguments for logging
	log         *slog.Logger
}

// Starts the worker loop which reads events from sources indefinitely or until
// bounded sources all return an end-of-input signal.
func (w *Worker) Start(ctx context.Context) error {
	w.log.Info("starting worker processes", w.diagnostics...)

	eg, gctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		if err := w.SourceRunner.Start(gctx); err != nil {
			return fmt.Errorf("worker source runner: %v", err)
		}
		return nil
	})
	eg.Go(func() error {
		if err := w.Operator.Start(gctx); err != nil {
			return fmt.Errorf("worker operator: %v", err)
		}
		return nil
	})
	if err := eg.Wait(); err != nil {
		return fmt.Errorf("worker start returned: %v", err)
	}

	w.log.Info("stopping", w.diagnostics...)
	return nil
}

// Stop is called externally in tests to gracefully stop worker processes.
func (w *Worker) Stop() error {
	srErr := w.SourceRunner.Stop()
	opErr := w.Operator.Stop()
	return errors.Join(srErr, opErr)
}

// Halt is called to immediately terminate processes without giving them a
// chance to deregister.
func (w *Worker) Halt() {
	w.SourceRunner.Halt()
	w.Operator.Halt()
}
