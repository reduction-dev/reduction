package jobstest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"runtime/debug"

	"golang.org/x/sync/errgroup"
	"reduction.dev/reduction/clocks"
	cfg "reduction.dev/reduction/config"
	"reduction.dev/reduction/jobs"
	"reduction.dev/reduction/rpc"
	"reduction.dev/reduction/storage"

	"connectrpc.com/connect"
)

// Create a new local job server for testing.
func NewServer(jd *cfg.Config, rpcListener, uiListener net.Listener, options ...func(*jobs.NewParams)) *Server {
	logger := slog.With("instanceID", "job")
	jobParams := &jobs.NewParams{
		JobConfig: jd,
		Clock:     clocks.NewSystemClock(),
		Logger:    logger,
	}
	for _, o := range options {
		o(jobParams)
	}
	job := jobs.New(jobParams)

	mux := http.NewServeMux()
	mux.Handle(rpc.NewJobUIConnectHandler(job))
	uiServer := &http.Server{Handler: mux}

	mux = http.NewServeMux()
	// Configure job to use JSON when creating worker clients for easier debugging
	// in tests.
	mux.Handle(rpc.NewJobConnectHandler(job, connect.WithProtoJSON()))
	rpcServer := &http.Server{Handler: mux}

	svr := &Server{
		uiServer:    uiServer,
		uiListener:  uiListener,
		rpcServer:   rpcServer,
		rpcListener: rpcListener,
		job:         job,
		log:         logger,
	}
	return svr
}

type ServerParams struct {
	rpcAddr    string
	uiAddr     string
	jobOptions []func(*jobs.NewParams)
}

// Create and run a local job server without blocking. Panics on any error.
func Run(jd *cfg.Config, options ...func(*ServerParams)) (server *Server, stop func()) {
	params := &ServerParams{
		rpcAddr: ":0",
		uiAddr:  ":0",
	}
	for _, o := range options {
		o(params)
	}

	rpcListener, err := net.Listen("tcp", params.rpcAddr)
	if err != nil {
		panic(err)
	}

	uiListener, err := net.Listen("tcp", params.uiAddr)
	if err != nil {
		panic(err)
	}

	if err := jd.Validate(); err != nil {
		slog.Error("invalid job config", "err", err, "trace", string(debug.Stack()))
		os.Exit(1)
	}

	server = NewServer(jd, rpcListener, uiListener, params.jobOptions...)

	go func() {
		err := server.Start()
		if err != nil {
			panic(err)
		}
	}()
	return server, func() {
		err := server.Stop()
		if err != nil {
			panic(err)
		}
	}
}

func WithSavepoint(uri string) func(*ServerParams) {
	return func(p *ServerParams) {
		p.jobOptions = append(p.jobOptions, func(np *jobs.NewParams) {
			np.SavepointURI = uri
		})
	}
}

func WithClock(clock clocks.Clock) func(*ServerParams) {
	return func(p *ServerParams) {
		p.jobOptions = append(p.jobOptions, func(np *jobs.NewParams) {
			np.Clock = clock
		})
	}
}

func WithStore(fs storage.FileStore) func(*ServerParams) {
	return func(p *ServerParams) {
		p.jobOptions = append(p.jobOptions, func(np *jobs.NewParams) {
			np.Store = fs
		})
	}
}

func WithRPCAddr(addr string) func(*ServerParams) {
	return func(sp *ServerParams) {
		sp.rpcAddr = addr
	}
}

type Server struct {
	uiServer    *http.Server
	uiListener  net.Listener
	rpcServer   *http.Server
	rpcListener net.Listener
	job         *jobs.Job
	log         *slog.Logger
}

func (s *Server) Start() error {
	s.log.Info("starting server",
		"rpcAddr", s.rpcListener.Addr().String(),
		"uiAddr", s.uiListener.Addr().String())

	g, ctx := errgroup.WithContext(context.Background())

	g.Go(func() error {
		go func() {
			<-ctx.Done()
			s.uiServer.Shutdown(context.Background())
		}()

		if err := s.uiServer.Serve(s.uiListener); err != http.ErrServerClosed {
			return fmt.Errorf("UI Server stopped: %v", err)
		}
		return nil
	})

	g.Go(func() error {
		go func() {
			<-ctx.Done()
			s.rpcListener.Close()
		}()

		if err := s.rpcServer.Serve(s.rpcListener); err != http.ErrServerClosed {
			return fmt.Errorf("job RPC server stopped: %v", err)
		}
		return nil
	})

	return g.Wait()
}

func (s *Server) RPCAddr() string {
	return s.rpcListener.Addr().String()
}

func (s *Server) RESTAddr() string {
	return s.uiListener.Addr().String()
}

func (s *Server) Stop() error {
	s.log.Info("stopping server")
	var err error
	if s.rpcServer != nil {
		err = s.rpcServer.Shutdown(context.Background())
	}
	if s.uiServer != nil {
		err = errors.Join(err, s.uiServer.Shutdown(context.Background()))
	}

	return err
}
