package workerstest

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"
	"reduction.dev/reduction/clocks"
	"reduction.dev/reduction/proto"
	"reduction.dev/reduction/proto/jobpb"
	"reduction.dev/reduction/rpc"
	"reduction.dev/reduction/rpc/batching"
	"reduction.dev/reduction/workers"

	"connectrpc.com/connect"
)

type server struct {
	worker     *workers.Worker
	httpServer *http.Server
	log        *slog.Logger
	listener   net.Listener
}

type NewServerParams struct {
	HandlerAddr string       // The address used to reach the handler function
	JobAddr     string       // The address used to reach the job server
	Clock       clocks.Clock // An optional clock used for testing
	LogPrefix   string       // Optional log prefix for differentiating different worker logs
}

func NewServer(t *testing.T, params NewServerParams) *server {
	// Create job client using JSON for easier debugging in tests
	job := rpc.NewJobConnectClient(params.JobAddr, connect.WithProtoJSON())

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on address: %v", err)
	}

	worker := workers.New(workers.NewParams{
		Host: listener.Addr().String(),
		Handler: rpc.NewHandlerConnectClient(rpc.NewHandlerConnectClientParams{
			Host: params.HandlerAddr,
			BatchingOptions: batching.EventBatcherParams{
				MaxSize:  100,
				MaxDelay: 5 * time.Millisecond,
			},
		}),
		Job:         job,
		Diagnostics: []any{"handler", params.HandlerAddr},
		Clock:       params.Clock,
		LogPrefix:   params.LogPrefix,
		OperatorFactory: func(senderID string, node *jobpb.NodeIdentity, errChan chan<- error) proto.Operator {
			return rpc.NewOperatorConnectClient(rpc.NewOperatorConnectClientParams{
				SenderID:     senderID,
				OperatorNode: node,
				BatchingOptions: batching.EventBatcherParams{
					MaxSize:  2,
					MaxDelay: 5 * time.Millisecond,
				},
			})
		},
	})

	mux := http.NewServeMux()

	path, handler := rpc.NewSourceRunnerConnectHandler(worker.SourceRunner)
	mux.Handle(path, handler)

	path, handler = rpc.NewOperatorConnectHandler(worker.Operator)
	mux.Handle(path, handler)

	return &server{
		worker:     worker,
		listener:   listener,
		httpServer: &http.Server{Handler: mux},
		log:        slog.With("instanceID", "worker"),
	}
}

// Create and run a test server. Any errors fail the test.
func Run(t *testing.T, params NewServerParams) (server *server, stop func()) {
	server = NewServer(t, params)
	go func() {
		err := server.Start(context.Background())
		if err != nil {
			t.Errorf("server.Start error: %v", err)
		}
	}()
	return server, func() {
		err := server.Stop()
		if err != nil {
			t.Fatalf("server.Stop error: %v", err)
		}
	}
}

func (s *server) Start(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	// Start the worker event loop
	g.Go(func() error {
		if err := s.worker.Start(ctx); err != nil {
			return fmt.Errorf("worker loop failed: %v", err)
		}
		return nil
	})

	// Start the http server
	g.Go(func() error {
		// Shutdown the server when context canceled
		go func() {
			<-ctx.Done()
			s.httpServer.Shutdown(context.Background())
		}()

		s.log.Info("starting worker server", "addr", s.listener.Addr().String())
		if err := s.httpServer.Serve(s.listener); err != http.ErrServerClosed {
			return fmt.Errorf("failed to start http server: %w", err)
		}
		return nil
	})

	return g.Wait()
}

func (s *server) Addr() string {
	return s.listener.Addr().String()
}

func (s *server) Stop() error {
	s.log.Info("worker server shutdown", "addr", s.Addr())
	if err := s.worker.Stop(); err != nil {
		return fmt.Errorf("worker server stopped: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.httpServer.Shutdown(ctx); err != nil {
		s.log.Error("requestListener.Stop error", "err", err)
	}

	return nil
}

// Immediately stop the server, preventing graceful shutdown.
func (s *server) Halt() {
	s.log.Info("Halt")
	s.httpServer.Close()
	s.worker.Halt()
}
