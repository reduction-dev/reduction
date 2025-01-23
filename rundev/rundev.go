package rundev

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"
	"reduction.dev/reduction/config"
	"reduction.dev/reduction/jobs/jobserver"
	"reduction.dev/reduction/workers/workerserver"
)

type RunParams struct {
	WorkerPort int
}

// Run starts a local cluster of job, worker, and handler for local development
// feedback.
func Run(params RunParams) error {
	// Stop everything on ctrl-c
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	g, ctx := errgroup.WithContext(ctx)

	// Start the handler first and wait for it to become healthy.
	g.Go(func() error {
		return handlerCmd(ctx).Run()
	})
	if err := waitUntilHealthy(ctx, ":8080", 100*time.Millisecond, 2*time.Second); err != nil {
		cancel(err)
		return err
	}

	// Start the job
	job, err := jobServer()
	if err != nil {
		return err
	}
	g.Go(func() error {
		return job.Start(ctx)
	})

	// Start the worker
	addr := fmt.Sprintf(":%d", params.WorkerPort)
	worker := workerserver.NewServer(workerserver.NewServerParams{
		Addr:         addr,
		JobAddr:      job.RPCListener.Addr().String(), // Use job's random port
		HandlerAddr:  ":8080",
		DBDir:        "./storage/dkv",
		SavepointDir: "./storage/savepoints",
	})
	g.Go(func() error {
		return worker.Start(ctx)
	})

	return g.Wait()
}

func handlerCmd(ctx context.Context) *exec.Cmd {
	cmd := exec.CommandContext(ctx, "./run")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	// Kill all child processes when canceled.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
	}

	return cmd
}

func jobServer() (*jobserver.Server, error) {
	data, err := os.ReadFile("./job.json")
	if err != nil {
		return nil, err
	}
	c, err := config.Unmarshal(data)
	if err != nil {
		return nil, err
	}

	if err := c.Validate(); err != nil {
		return nil, fmt.Errorf("job definition validation error: %v", err)
	}

	server := jobserver.NewServer(c, jobserver.WithRPCAddress(":0"), jobserver.WithUIAddress(":9009"))
	return server, nil
}
