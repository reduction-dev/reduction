package rundev

import (
	"context"
	"encoding/json"
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
	Executable string
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

	// Get config from executable first
	jconf, err := jobConfig(ctx, params.Executable)
	if err != nil {
		return fmt.Errorf("failed to get job config: %v", err)
	}

	// Start the job server
	jobServer := jobserver.NewServer(jconf, jobserver.WithRPCAddress(":0"), jobserver.WithUIAddress(":9009"))
	g.Go(func() error {
		return jobServer.Start(ctx)
	})

	// Start the handler process
	g.Go(func() error {
		return handlerCmd(ctx, params.Executable).Run()
	})
	if err := waitUntilHealthy(ctx, "http://localhost:8080", 100*time.Millisecond, 2*time.Second); err != nil {
		cancel(err)
		return err
	}

	// Start the worker
	addr := fmt.Sprintf(":%d", params.WorkerPort)
	worker := workerserver.NewServer(workerserver.NewServerParams{
		Addr:         addr,
		JobAddr:      jobServer.RPCListener.Addr().String(), // Use job's random port
		HandlerAddr:  "localhost:8080",                      // The Node connect server needs the hostname
		DBDir:        "./storage/dkv",
		SavepointDir: "./storage/savepoints",
	})
	g.Go(func() error {
		return worker.Start(ctx)
	})

	return g.Wait()
}

func handlerCmd(ctx context.Context, executable string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, executable, "start")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	// Kill all child processes when canceled.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
	}

	return cmd
}

func jobConfig(ctx context.Context, executable string) (*config.Config, error) {
	cmd := exec.CommandContext(ctx, executable, "config")
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to get config from executable: %v", err)
	}

	if !json.Valid(output) {
		return nil, fmt.Errorf("invalid JSON config output from executable")
	}

	c, err := config.Unmarshal(output, config.NewParams())
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %v", err)
	}

	if err := c.Validate(); err != nil {
		return nil, fmt.Errorf("job definition validation error: %v", err)
	}

	return c, nil
}
