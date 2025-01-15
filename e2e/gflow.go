package e2e

import (
	"net"

	"reduction.dev/reduction-go/rxn"
)

// One command to start and run an http server for a user handler. Any errors panic.
func RunHandler(handler rxn.Handler) (server *rxn.HTTPServer, stop func()) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}

	server = rxn.NewServer(handler, rxn.WithListener(listener))
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
