package e2e

import (
	"net"

	"reduction.dev/reduction-go/jobs"
	"reduction.dev/reduction-go/rxn"
)

// One command to start and run an http server for a user handler. Any errors panic.
func RunHandler(jobDef *jobs.Job) (server *rxn.HTTPServer, stop func()) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}
	synthesis, err := jobDef.Synthesize()
	if err != nil {
		panic(err)
	}

	server = rxn.NewServer(synthesis.Handler, rxn.WithListener(listener))
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
