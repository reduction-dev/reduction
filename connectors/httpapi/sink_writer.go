package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

type SinkWriter struct {
	addr       string
	httpClient *http.Client
}

// Create a new HTTPAPI sink that writes records to the configured host address.
func NewSink(config SinkConfig) *SinkWriter {
	return &SinkWriter{
		addr:       config.Addr,
		httpClient: &http.Client{},
	}
}

// The expected JSON type to pass to Write. This type must match the type used
// in the handlers when sending sink records.
type HTTPSinkEvent struct {
	// A namespace for writing the record
	Topic string
	// Base64 encoded data
	Data []byte
}

// Write a single event record to the sink. The event param must be a JSON
// representation of the HTTPSinkEvent type.
func (s *SinkWriter) Write(event []byte) error {
	// Decode JSON
	var sinkEvent HTTPSinkEvent
	err := json.Unmarshal(event, &sinkEvent)
	if err != nil {
		return fmt.Errorf("httpapi.Write failed json unmarshal: %w", err)
	}
	if sinkEvent.Topic == "" {
		return errors.New("httpapi.Write HTTPSinkEvent missing topic")
	}

	// Encode event list. The Write method accepts 1 event, but we write with an
	// list of events to enable batching via the http API later.
	eventList, err := json.Marshal([][]byte{sinkEvent.Data})
	if err != nil {
		return fmt.Errorf("httpapi.Write failed eventList Marshal: %w", err)
	}
	data := bytes.NewBuffer(eventList)

	// Create request
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	url := s.addr + "/topics/" + sinkEvent.Topic
	req, err := http.NewRequestWithContext(ctx, "POST", url, data)
	if err != nil {
		return err
	}

	// Send request
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode >= 400 {
		msg, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed http request: %d response from %s, %s", resp.StatusCode, url, msg)
	}

	return nil
}
