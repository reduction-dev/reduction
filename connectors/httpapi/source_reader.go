package httpapi

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/proto/workerpb"
	"reduction.dev/reduction/util/sliceu"
)

type SourceReader struct {
	hostAddr   string
	httpClient *http.Client
	topics     []string
	splits     []*split
	wasEmpty   bool // Keep track of whether last response was an empty list of events
}

type split struct {
	cursor  int
	splitID string
	eoi     bool
}

func NewSourceReader(config SourceConfig) *SourceReader {
	return &SourceReader{
		hostAddr: config.Addr,
		httpClient: &http.Client{
			Timeout: time.Duration(1) * time.Second,
		},
		topics: config.Topics,
	}
}

type HTTPSourceEvent struct {
	Events [][]byte
	Status string
	Cursor int
}

func (s *SourceReader) ReadEvents() ([][]byte, error) {
	if len(s.topics) != 1 {
		return nil, fmt.Errorf("currently need exactly 1 topic but got %d: %v", len(s.topics), s.topics)
	}

	// If the last read was empty, wait before reading again.
	if s.wasEmpty {
		time.Sleep(100 * time.Millisecond)
		s.wasEmpty = false
	}

	// Read from each assigned split.
	var events [][]byte
	for _, split := range s.splits {
		resp, err := s.httpClient.Get(s.hostAddr + "/topics/" + s.topics[0] + "/" + strconv.Itoa(split.cursor))
		if err != nil {
			return nil, fmt.Errorf("httpapi ReadEvents GET: %w", err)
		}
		if resp.StatusCode >= 400 {
			return nil, fmt.Errorf("httpapi ReadEvents GET http error code: %d", resp.StatusCode)
		}

		decoder := json.NewDecoder(resp.Body)

		var sourceEvent HTTPSourceEvent
		err = decoder.Decode(&sourceEvent)
		if err != nil {
			return nil, fmt.Errorf("httpapi ReadEvents Decode: %w", err)
		}

		if sourceEvent.Status == "eoi" {
			split.eoi = true
		}

		split.cursor = sourceEvent.Cursor
		events = append(events, sourceEvent.Events...)
	}

	// Determine if all splits reached end of input
	allEOI := sliceu.Every(s.splits, func(s *split) bool { return s.eoi })
	if allEOI {
		return events, connectors.ErrEndOfInput
	}

	if len(events) == 0 {
		s.wasEmpty = true
	}

	return events, nil
}

func (s *SourceReader) AssignSplits(splits []*workerpb.SourceSplit) error {
	if len(splits) > 1 {
		return fmt.Errorf("httpapi.SourceReader can handle at most one split currently but got %d", len(splits))
	}

	for _, sp := range splits {
		readerSplit := &split{splitID: sp.SplitId}

		// Read the binary cursor if available
		if len(sp.Cursor) != 0 {
			var cursor int64
			err := binary.Read(bytes.NewBuffer(sp.Cursor), binary.BigEndian, &cursor)
			if err != nil {
				return err
			}
			readerSplit.cursor = int(cursor)
		}

		s.splits = append(s.splits, readerSplit)
	}

	return nil
}

// Checkpoint returns a list of split cursors (int64) encoded as bytes
func (s *SourceReader) Checkpoint() [][]byte {
	result := make([][]byte, len(s.splits))
	for i, split := range s.splits {
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(split.cursor))
		result[i] = b
	}
	return result
}

var _ connectors.SourceReader = (*SourceReader)(nil)
