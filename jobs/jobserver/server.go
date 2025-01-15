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
	"reduction.dev/reduction/rpc"
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

	job := jobs.New(&jobs.NewParams{
		JobConfig: jd,
	})

	mux := http.NewServeMux()
	mux.Handle(rpc.NewJobUIConnectHandler(job))
	uiServer := httpu.NewServer(mux)
	uiListener, err := net.Listen("tcp", serverOptions.restAddr)
	if err != nil {
		panic(err)
	}

	mux = http.NewServeMux()
	mux.Handle(rpc.NewJobConnectHandler(job))
	rpcServer := httpu.NewServer(mux)
	rpcListener, err := net.Listen("tcp", serverOptions.rpcAddr)
	if err != nil {
		panic(err)
	}

	return &Server{
		uiServer:    uiServer,
		UIListener:  uiListener,
		rpcServer:   rpcServer,
		RPCListener: rpcListener,
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
	uiServer    *httpu.Server
	UIListener  net.Listener
	rpcServer   *httpu.Server
	RPCListener net.Listener
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
