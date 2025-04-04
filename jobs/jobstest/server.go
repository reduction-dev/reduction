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
	"time"

	"connectrpc.com/connect"
	"golang.org/x/sync/errgroup"
	"reduction.dev/reduction-go/topology"
	"reduction.dev/reduction/clocks"
	"reduction.dev/reduction/config"
	cfg "reduction.dev/reduction/config"
	"reduction.dev/reduction/jobs"
	"reduction.dev/reduction/proto"
	"reduction.dev/reduction/proto/jobpb"
	"reduction.dev/reduction/rpc"
	"reduction.dev/reduction/storage"
	"reduction.dev/reduction/storage/locations"
	"reduction.dev/reduction/storage/snapshots"
)

// Create a new local job server for testing.
func NewServer(jobConfig *cfg.Config, rpcListener, uiListener net.Listener, options ...func(*jobs.NewParams)) (*Server, error) {
	logger := slog.With("instanceID", "job")
	store, err := locations.New(jobConfig.WorkingStorageLocation)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize storage location %s: %w", jobConfig.WorkingStorageLocation, err)
	}

	jobParams := &jobs.NewParams{
		JobConfig: jobConfig,
		Clock:     clocks.NewSystemClock(),
		Logger:    logger,
		OperatorFactory: func(senderID string, node *jobpb.NodeIdentity) proto.Operator {
			return rpc.NewOperatorConnectClient(rpc.NewOperatorConnectClientParams{
				SenderID:       senderID,
				OperatorNode:   node,
				ConnectOptions: []connect.ClientOption{connect.WithProtoJSON()},
			})
		},
		SourceRunnerFactory: func(node *jobpb.NodeIdentity) proto.SourceRunner {
			return rpc.NewSourceRunnerConnectClient(node, connect.WithProtoJSON())
		},
		CheckpointEvents: make(chan snapshots.CheckpointEvent, 1),
		Store:            store,
	}
	for _, o := range options {
		o(jobParams)
	}
	job := jobs.New(jobParams)

	mux := http.NewServeMux()
	mux.Handle(rpc.NewJobUIConnectHandler(job))
	uiServer := &http.Server{Handler: mux}

	// Configure job to use JSON when creating worker clients for easier debugging
	// in tests.
	jobPath, jobHandler := rpc.NewJobConnectHandler(job)
	mux = http.NewServeMux()
	mux.Handle(jobPath, jobHandler)
	rpcServer := &http.Server{Handler: mux}

	svr := &Server{
		uiServer:         uiServer,
		uiListener:       uiListener,
		rpcServer:        rpcServer,
		rpcListener:      rpcListener,
		job:              job,
		log:              logger,
		checkpointEvents: jobParams.CheckpointEvents,
	}
	return svr, nil
}

type ServerParams struct {
	rpcAddr    string
	uiAddr     string
	jobOptions []func(*jobs.NewParams)
}

// Create and run a local job server without blocking. Panics on any error.
func Run(jd *topology.Job, options ...func(*ServerParams)) (server *Server, stop func()) {
	synthesis, err := jd.Synthesize()
	if err != nil {
		panic(err)
	}
	jobConfig, err := cfg.Unmarshal(synthesis.Config.Marshal(), config.NewParams())
	if err != nil {
		panic(err)
	}

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

	if err := jobConfig.Validate(); err != nil {
		slog.Error("invalid job config", "err", err, "trace", string(debug.Stack()))
		os.Exit(1)
	}

	server, err = NewServer(jobConfig, rpcListener, uiListener, params.jobOptions...)
	if err != nil {
		panic(fmt.Sprintf("failed to create job server: %v", err))
	}

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
	uiServer         *http.Server
	uiListener       net.Listener
	rpcServer        *http.Server
	rpcListener      net.Listener
	job              *jobs.Job
	log              *slog.Logger
	checkpointEvents chan snapshots.CheckpointEvent
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

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return s.rpcServer.Shutdown(gctx)
	})

	g.Go(func() error {
		return s.uiServer.Shutdown(gctx)
	})

	if err := g.Wait(); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("server shutdown error: %v", err)
	}
	return nil
}
