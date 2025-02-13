package main

import (
	"fmt"
	"log"
	"log/slog"
	"os"

	"github.com/urfave/cli/v2"
	cfg "reduction.dev/reduction/config"
	"reduction.dev/reduction/jobs/jobserver"
	"reduction.dev/reduction/logging"
	"reduction.dev/reduction/rundev"
	"reduction.dev/reduction/testrun"
	"reduction.dev/reduction/workers/workerserver"
)

func main() {
	app := &cli.App{
		Name:  "reduction",
		Usage: "Aggregate information from streams of events",
		Commands: []*cli.Command{{
			Name:  "job",
			Usage: "Start a Reduction Job",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:  "admin-port",
					Value: "127.0.0.1:8080",
					Usage: "the job server will listen for admin requests on this port",
				},
				&cli.StringFlag{
					Name:  "cluster-port",
					Value: "127.0.0.1:8081",
					Usage: "the job server will listen for internal requests from cluster nodes on this port",
				},
			},
			Action: func(ctx *cli.Context) error {
				configPath := ctx.Args().First()
				return startJobServer(configPath, ctx.Int("admin-port"), ctx.Int("cluster-port"))
			},
		}, {
			Name:  "worker",
			Usage: "Start a Reduction worker",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:     "job-addr",
					Value:    "127.0.0.1:8081",
					Usage:    "the address used to call the job server.",
					Required: true,
				},
				&cli.StringFlag{
					Name:     "handler-addr",
					Value:    "127.0.0.1:8080",
					Usage:    "the address used to call the handler server.",
					Required: true,
				},
				&cli.IntFlag{
					Name:     "port",
					Value:    0,
					Usage:    "specify the worker port to run on",
					Required: false,
				},
			},
			Action: func(ctx *cli.Context) error {
				jobAddr := ctx.String("job-addr")
				handlerAddr := ctx.String("handler-addr")
				port := ctx.Int("port")
				return startWorkerServer(port, jobAddr, handlerAddr)
			},
		}, {
			Name:  "dev",
			Usage: "Start a self-contained cluster for local development",
			Flags: []cli.Flag{
				&cli.IntFlag{
					Name:     "worker-port",
					Value:    0,
					Usage:    "the address used to call the worker server.",
					Required: false,
				},
			},
			Args:      true,
			ArgsUsage: "<executable>",
			Action: func(ctx *cli.Context) error {
				logging.SetLevel(slog.LevelWarn)
				slog.SetDefault(slog.New(logging.NewTextHandler()))
				executable := ctx.Args().First()
				if executable == "" {
					return fmt.Errorf("executable path is required")
				}
				port := ctx.Int("worker-port")
				err := rundev.Run(rundev.RunParams{
					WorkerPort: port,
					Executable: executable,
				})
				if err != nil {
					slog.Error("terminated with error", "error", err)
				}
				return err
			},
		}, {
			Name:  "testrun",
			Usage: "Run an integration test against a handler over stdin/stdout",
			Action: func(ctx *cli.Context) error {
				slog.SetDefault(slog.New(logging.NewTextHandler()))
				if err := testrun.Run(os.Stdin, os.Stdout); err != nil {
					slog.Error("terminated with error", "error", err)
					return err
				}
				return nil
			},
		}},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func startJobServer(jobPath string, adminPort, clusterPort int) error {
	data, err := os.ReadFile(jobPath)
	if err != nil {
		return err
	}
	c, err := cfg.Unmarshal(data)
	if err != nil {
		return err
	}

	if err := c.Validate(); err != nil {
		return fmt.Errorf("job definition validation error: %v", err)
	}

	var opts []jobserver.Option
	if adminPort != 0 {
		opts = append(opts, jobserver.WithUIAddress(fmt.Sprintf(":%d", adminPort)))
	}
	if clusterPort != 0 {
		opts = append(opts, jobserver.WithRPCAddress(fmt.Sprintf(":%d", clusterPort)))
	}

	return jobserver.Run(c, opts...)
}

func startWorkerServer(port int, jobAddr, handlerAddr string) error {
	return workerserver.Run(workerserver.NewServerParams{
		Addr:         fmt.Sprintf(":%d", port),
		JobAddr:      jobAddr,
		HandlerAddr:  handlerAddr,
		DBDir:        "./dkv",
		SavepointDir: "./savepoints",
	})
}
