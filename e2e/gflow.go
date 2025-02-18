package e2e

import (
	"context"
	"net"
	"time"

	"reduction.dev/reduction-go/jobs"
	"reduction.dev/reduction-go/rxnsvr"
)

// One command to start and run an http server for a user handler. Any errors panic.
func RunHandler(jobDef *jobs.Job) (server *rxnsvr.Server, stop func()) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}
	synthesis, err := jobDef.Synthesize()
	if err != nil {
		panic(err)
	}

	server = rxnsvr.New(synthesis.Handler, rxnsvr.WithListener(listener))
	go func() {
		err := server.Start()
		if err != nil {
			panic(err)
		}
	}()
	return server, func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		err := server.Stop(ctx)
		if err != nil && err != context.DeadlineExceeded {
			panic(err)
		}
	}
}
