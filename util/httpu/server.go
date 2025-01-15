package httpu

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"runtime"
)

type handlerWithContext struct {
	http.Handler
	cancel context.CancelCauseFunc
}

func (h handlerWithContext) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer func() {
		err := recover()
		if err != nil {
			buf := make([]byte, 1<<20)
			n := runtime.Stack(buf, true)
			fmt.Fprintf(os.Stderr, "panic: %v\n\n%s", err, buf[:n])
			cancelErr, ok := err.(error)
			if ok {
				h.cancel(cancelErr)
			} else {
				h.cancel(nil)
			}
		}
	}()
	h.Handler.ServeHTTP(w, r)
}

type Server struct {
	http.Server
}

func NewServer(handler http.Handler) *Server {
	return &Server{
		http.Server{
			Handler: handler,
		},
	}
}

// Serve acts like an (nil)(*http.Server).Serve method but also includes a
// context argument. The server will shutdown when the context is canceled. The
// server will also shutdown if the http.Handler panics. This is unlike the
// typical http.Server behavior which recovers panics and continues processing.
func (s *Server) Serve(ctx context.Context, l net.Listener) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	s.Server.Handler = handlerWithContext{s.Server.Handler, cancel}

	go func() {
		<-ctx.Done()
		s.Server.Shutdown(ctx)
	}()

	return s.Server.Serve(l)
}
