package logging

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
)

type loggingHandler struct {
	httpHandler http.Handler
	log         *slog.Logger
}

func NewHTTPHandler(h http.Handler, logger *slog.Logger) http.Handler {
	return &loggingHandler{
		httpHandler: h,
		log:         logger,
	}
}

func (h *loggingHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost || r.Method == http.MethodPut {
		// Read the body and replace it on request
		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "can't read body", http.StatusBadRequest)
			return
		}
		r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

		h.log.Info("request", "method", r.Method, "path", r.URL.Path, "body", string(bodyBytes))
	} else {
		h.log.Info("request", "method", r.Method, "path", r.URL.Path)
	}
	h.httpHandler.ServeHTTP(w, r)
}
