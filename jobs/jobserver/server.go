package jobserver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"

	cfg "reduction.dev/reduction/config"
	"reduction.dev/reduction/jobs"
	"reduction.dev/reduction/proto"
	"reduction.dev/reduction/proto/jobpb"
	"reduction.dev/reduction/rpc"
	"reduction.dev/reduction/storage/snapshots"
	"reduction.dev/reduction/util/httpu"

	"golang.org/x/sync/errgroup"
)

type Option func(*serverOptions)

type serverOptions struct {
	restAddr string
	rpcAddr  string
}

// Create a new local job server for testing.
func NewServer(jd *cfg.Config, options ...Option) *Server {
	// Put functional options into a struct
	serverOptions := &serverOptions{}
	for _, o := range options {
		o(serverOptions)
	}

	// Default REST UI port to 8080
	if serverOptions.restAddr == "" {
		serverOptions.restAddr = "127.0.0.1:8080"
	}

	// Default internal RPC port to 8081
	if serverOptions.rpcAddr == "" {
		serverOptions.rpcAddr = "127.0.0.1:8081"
	}

	// A channel for handling checkpoint errors
	checkpointEvents := make(chan snapshots.CheckpointEvent, 1)

	job := jobs.New(&jobs.NewParams{
		JobConfig: jd,
		OperatorFactory: func(senderID string, node *jobpb.NodeIdentity, errChan chan<- error) proto.Operator {
			return rpc.NewOperatorConnectClient(rpc.NewOperatorConnectClientParams{
				SenderID:     senderID,
				OperatorNode: node,
			})
		},
		SourceRunnerFactory: func(node *jobpb.NodeIdentity) proto.SourceRunner {
			return rpc.NewSourceRunnerConnectClient(node)
		},
		CheckpointEvents: checkpointEvents,
	})

	mux := http.NewServeMux()
	mux.Handle(rpc.NewJobUIConnectHandler(job))
	uiServer := httpu.NewServer(mux)
	uiListener, err := net.Listen("tcp", serverOptions.restAddr)
	if err != nil {
		panic(err)
	}

	jobPath, jobHandler := rpc.NewJobConnectHandler(job)
	mux = http.NewServeMux()
	mux.Handle(jobPath, jobHandler)
	rpcServer := httpu.NewServer(mux)
	rpcListener, err := net.Listen("tcp", serverOptions.rpcAddr)
	if err != nil {
		panic(err)
	}

	return &Server{
		uiServer:         uiServer,
		UIListener:       uiListener,
		rpcServer:        rpcServer,
		RPCListener:      rpcListener,
		checkpointEvents: checkpointEvents,
	}
}

// Create and run a local job server. Blocks.
func Run(jd *cfg.Config, options ...Option) error {
	server := NewServer(jd, options...)
	return server.Start(context.Background())
}

func WithRPCAddress(addr string) func(*serverOptions) {
	return func(o *serverOptions) {
		o.rpcAddr = addr
	}
}

func WithUIAddress(addr string) func(*serverOptions) {
	return func(js *serverOptions) {
		js.restAddr = addr
	}
}

type Server struct {
	uiServer         *httpu.Server
	UIListener       net.Listener
	rpcServer        *httpu.Server
	RPCListener      net.Listener
	checkpointEvents chan snapshots.CheckpointEvent
}

func (s *Server) Start(ctx context.Context) error {
	slog.Info("starting job server",
		"rpcAddr", s.RPCListener.Addr().String(),
		"restAddr", s.UIListener.Addr().String())

	g, gctx := errgroup.WithContext(ctx)

	// Start the UI server
	g.Go(func() error {
		if err := s.uiServer.Serve(gctx, s.UIListener); err != http.ErrServerClosed {
			return fmt.Errorf("job ui server stopped: %v", err)
		}
		return nil
	})

	// Start the RPC Server
	g.Go(func() error {
		if err := s.rpcServer.Serve(gctx, s.RPCListener); err != http.ErrServerClosed {
			return fmt.Errorf("job RPC server stopped: %w", err)
		}
		return nil
	})

	// Listen for checkpoint errors
	g.Go(func() error {
		for {
			select {
			case event := <-s.checkpointEvents:
				if event.Err != nil {
					return fmt.Errorf("checkpoint error: %w", event.Err)
				}
			case <-gctx.Done():
				return nil
			}
		}
	})

	return g.Wait()
}

func (s *Server) Stop() error {
	slog.Info("stopping job server")
	var err error
	if s.rpcServer != nil {
		err = s.rpcServer.Shutdown(context.Background())
	}
	if s.uiServer != nil {
		err = errors.Join(err, s.uiServer.Shutdown(context.Background()))
	}

	return err
}
