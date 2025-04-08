package workerserver

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/errgroup"
	"reduction.dev/reduction/batching"
	"reduction.dev/reduction/proto"
	"reduction.dev/reduction/proto/jobpb"
	"reduction.dev/reduction/rpc"
	"reduction.dev/reduction/telemetry"
	"reduction.dev/reduction/util/httpu"
	"reduction.dev/reduction/util/netu"
	"reduction.dev/reduction/workers"

	"connectrpc.com/connect"
)

type server struct {
	worker      *workers.Worker
	httpServer  *httpu.Server
	log         *slog.Logger
	listener    net.Listener
	diagnostics []any
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
	// An override to set the worker's host rather then trying to resolve it
	Host string
}

func NewServer(params NewServerParams) *server {
	// Create job client using JSON for easier debugging in tests
	job := rpc.NewJobConnectClient(params.JobAddr, connect.WithProtoJSON())

	logger := slog.With("instanceID", "worker")

	diagnostics := []any{}

	// Resolve the address the other nodes can reach this node at.
	if params.Host == "" {
		params.Host = netu.NodeAddress()
	}
	diagnostics = append(diagnostics, "nodeHost", params.Host)

	// Create a listener so that the final port is known if 0 is passed.
	listener, err := net.Listen("tcp", params.Addr)
	if err != nil {
		panic(fmt.Sprintf("failed to listen on address: %v", err))
	}
	diagnostics = append(diagnostics, "listeningAddr", listener.Addr().String())

	// Get the port used by the listener
	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		panic(fmt.Sprintf("failed to get port from listener address: %v", err))
	}

	worker := workers.New(workers.NewParams{
		Host: fmt.Sprintf("%s:%s", params.Host, port),
		Handler: rpc.NewHandlerConnectClient(rpc.NewHandlerConnectClientParams{
			Host: params.HandlerAddr,
			Opts: []connect.ClientOption{
				connect.WithInterceptors(
					rpc.NewLoggingInterceptor(logger),
					telemetry.NewRPCInterceptor("handler"),
				),
			},
			BatchingOptions: batching.EventBatcherParams{
				MaxSize:  100,
				MaxDelay: 10 * time.Millisecond,
			},
		}),
		Job:         job,
		Diagnostics: []any{"handler", params.HandlerAddr},
		OperatorFactory: func(senderID string, node *jobpb.NodeIdentity) proto.Operator {
			return rpc.NewOperatorConnectClient(rpc.NewOperatorConnectClientParams{
				SenderID:     senderID,
				OperatorNode: node,
				ConnectOptions: []connect.ClientOption{
					connect.WithInterceptors(telemetry.NewRPCInterceptor("operator")),
				},
			})
		},
		EventBatching: batching.EventBatcherParams{
			MaxSize:  100,
			MaxDelay: 10 * time.Millisecond,
		},
	})

	mux := http.NewServeMux()

	// Register pprof handlers
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	// Prometheus metrics
	mux.Handle("/metrics", promhttp.Handler())

	path, handler := rpc.NewSourceRunnerConnectHandler(worker.SourceRunner)
	mux.Handle(path, handler)

	path, handler = rpc.NewOperatorConnectHandler(worker.Operator)
	mux.Handle(path, handler)

	return &server{
		listener:    listener,
		worker:      worker,
		httpServer:  httpu.NewServer(mux),
		log:         logger,
		diagnostics: diagnostics,
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
		s.log.Info("starting worker server", s.diagnostics...)

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
