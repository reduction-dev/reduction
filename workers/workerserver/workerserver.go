package workerserver

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"

	"golang.org/x/sync/errgroup"
	"reduction.dev/reduction/proto/jobpb"
	"reduction.dev/reduction/rpc"
	"reduction.dev/reduction/util/httpu"
	"reduction.dev/reduction/workers"

	"connectrpc.com/connect"
)

type server struct {
	worker     *workers.Worker
	httpServer *httpu.Server
	log        *slog.Logger
	listener   net.Listener
}

type NewServerParams struct {
	// Address to listen on for incoming requests
	Addr string
	// The address used to reach the handler function
	HandlerAddr string
	// The address used to reach the job server
	JobAddr string
	// A local path for storing DB files
	DBDir string
	// A local path for storing savepoint files
	SavepointDir string
}

func NewServer(params NewServerParams) *server {
	// Create job client using JSON for easier debugging in tests
	job := rpc.NewJobConnectClient(params.JobAddr, connect.WithProtoJSON())

	logger := slog.With("instanceID", "worker")

	// Create a listener so that the final port is known if 0 is passed.
	listener, err := net.Listen("tcp", params.Addr)
	if err != nil {
		panic(fmt.Sprintf("failed to listen on address: %v", err))
	}
	worker := workers.New(workers.NewParams{
		Host:        listener.Addr().String(),
		Handler:     rpc.NewHandlerConnectClient(params.HandlerAddr, connect.WithInterceptors(rpc.NewLoggingInterceptor(logger))),
		Job:         job,
		Diagnostics: []any{"handler", params.HandlerAddr},
	})

	mux := http.NewServeMux()

	// Register pprof handlers
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	path, handler := rpc.NewSourceRunnerConnectHandler(
		worker.SourceRunner,
		func(node *jobpb.NodeIdentity) *rpc.OperatorConnectClient {
			return rpc.NewOperatorConnectClient(node)
		},
	)
	mux.Handle(path, handler)

	path, handler = rpc.NewOperatorConnectHandler(worker.Operator)
	mux.Handle(path, handler)

	return &server{
		listener:   listener,
		worker:     worker,
		httpServer: httpu.NewServer(mux),
		log:        logger,
	}
}

// Create and run a worker server.
func Run(params NewServerParams) error {
	server := NewServer(params)
	return server.Start(context.Background())
}

func (s *server) Start(ctx context.Context) error {
	g, gctx := errgroup.WithContext(ctx)

	// Start the worker event loop
	g.Go(func() error {
		if err := s.worker.Start(gctx); err != nil {
			return fmt.Errorf("worker loop failed: %v", err)
		}
		return nil
	})

	// Start the http server
	g.Go(func() error {
		s.log.Info("starting worker server", "addr", s.listener.Addr())

		if err := s.httpServer.Serve(gctx, s.listener); err != http.ErrServerClosed {
			return fmt.Errorf("failed to start http server: %w", err)
		}
		return nil
	})

	return g.Wait()
}

func (s *server) Stop() error {
	s.log.Info("worker server shutdown", "addr", s.listener.Addr())
	if err := s.httpServer.Shutdown(context.Background()); err != nil {
		s.log.Error("requestListener.Stop error", "err", err)
	}
	return s.worker.Stop()
}
