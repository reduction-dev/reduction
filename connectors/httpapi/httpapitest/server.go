package httpapitest

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"

	"reduction.dev/reduction/logging"
)

type Store struct {
	topics        map[string]*Topic
	readBatchSize int
	topicsMutext  sync.Mutex
	unbounded     bool
}

type Topic struct {
	Records [][]byte
}

type SourceStatus string

// Flag indicating the server has no more and will never have more records.
const SourceStatusEndOfInput SourceStatus = "eoi"

// Flag indicating that the server has more records beyond those it's responded
// with.
const SourceStatusMore SourceStatus = "more"

func (s *Store) Write(topic string, record []byte) {
	s.topicsMutext.Lock()
	defer s.topicsMutext.Unlock()

	t, ok := s.topics[topic]
	if !ok {
		t = &Topic{Records: make([][]byte, 0)}
		s.topics[topic] = t
	}
	t.Records = append(t.Records, record)
}

func (s *Store) Read(topic string, cursor int) ([][]byte, error) {
	noMoreRecords := io.EOF
	if s.unbounded {
		noMoreRecords = nil
	}

	s.topicsMutext.Lock()
	defer s.topicsMutext.Unlock()

	t, ok := s.topics[topic]
	if !ok {
		return nil, noMoreRecords
	}

	// Set the index to read up to. Default (0) reads all records.
	exclusiveEnd := len(t.Records)
	if s.readBatchSize > 0 {
		exclusiveEnd = cursor + s.readBatchSize
	}

	if exclusiveEnd >= len(t.Records) {
		return t.Records[cursor:len(t.Records)], noMoreRecords
	}
	return t.Records[cursor:exclusiveEnd], nil
}

func (s *Store) RecordCount(topic string) int {
	s.topicsMutext.Lock()
	defer s.topicsMutext.Unlock()

	t, ok := s.topics[topic]
	if !ok {
		return 0
	}
	return len(t.Records)
}

type ReadResponse struct {
	Events [][]byte
	Cursor int
	Status SourceStatus
}

type SinkServer struct {
	httpServer *httptest.Server
	httpClient *http.Client
	sink       *Store
}

func (s *SinkServer) Close() {
	s.httpServer.Close()
}

func (s *SinkServer) URL() string {
	return s.httpServer.URL
}

func (s *SinkServer) Read(topic string) *ReadResponse {
	t, ok := s.sink.topics[topic]
	if !ok {
		panic("tried to read from unwritten topic")
	}
	return &ReadResponse{
		Events: t.Records,
		Cursor: len(t.Records),
		Status: SourceStatusEndOfInput,
	}
}

func (s *SinkServer) RecordCount(topic string) int {
	return s.sink.RecordCount(topic)
}

// Convenience function for writing JSON records during a test without creating
// a Sink. This method doesn't use http and writes directly to the data source.
func (s *SinkServer) WriteJSON(topic string, events []any) {
	for _, e := range events {
		bs, err := json.Marshal(e)
		if err != nil {
			panic(fmt.Sprintf("httpapi.SinkServer Write marshal: %v", err))
		}
		s.sink.Write(topic, bs)
	}
}

func (s *SinkServer) Write(topic string, event []byte) {
	s.sink.Write(topic, event)
}

func (s *SinkServer) WriteBatch(topic string, events [][]byte) {
	for _, e := range events {
		s.sink.Write(topic, e)
	}
}

type ServerOption func(sink *Store)

func StartServer(options ...ServerOption) *SinkServer {
	sink := &Store{
		topics: make(map[string]*Topic),
	}
	for _, o := range options {
		o(sink)
	}

	mux := http.NewServeMux()

	mux.HandleFunc("POST /topics/{topicID}", func(w http.ResponseWriter, r *http.Request) {
		topicID := r.PathValue("topicID")
		if topicID == "" {
			http.Error(w, "missing topicID in request", http.StatusInternalServerError)
			return
		}
		v, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		var eventList [][]byte
		err = json.Unmarshal(v, &eventList)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		for _, e := range eventList {
			sink.Write(topicID, e)
		}
	})

	mux.HandleFunc("GET /topics/{topicID}/{cursor}", func(w http.ResponseWriter, r *http.Request) {
		topicID := r.PathValue("topicID")
		if topicID == "" {
			http.Error(w, "missing topicID in request", http.StatusInternalServerError)
			return
		}
		cursorParam := r.PathValue("cursor")
		var cursor int
		if cursorParam == "" {
			cursor = 0
		} else {
			var err error
			cursor, err = strconv.Atoi(cursorParam)
			if err != nil {
				http.Error(w, fmt.Sprintf("invalid cursor param: %s", cursorParam), http.StatusInternalServerError)
				return
			}
		}
		t, err := sink.Read(topicID, cursor)
		var status SourceStatus
		if err == io.EOF {
			status = SourceStatusEndOfInput
		} else {
			status = SourceStatusMore
		}
		response := ReadResponse{
			Events: t,
			Cursor: cursor + len(t),
			Status: status,
		}
		data, err := json.Marshal(response)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Write(data)
	})

	logger := slog.With("instanceID", "httpapi")
	handler := logging.NewHTTPHandler(mux, logger)

	httpServer := httptest.NewServer(handler)
	logger.Info("start", "addr", httpServer.URL)
	return &SinkServer{
		httpServer: httpServer,
		httpClient: &http.Client{},
		sink:       sink,
	}
}

func WithReadBatchSize(size int) ServerOption {
	return func(sink *Store) {
		sink.readBatchSize = size
	}
}

func WithUnboundedReading() ServerOption {
	return func(sink *Store) {
		sink.unbounded = true
	}
}
