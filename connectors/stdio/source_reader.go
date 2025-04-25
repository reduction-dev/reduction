package stdio

import (
	"bufio"
	"bytes"
	"io"
	"os"

	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/proto/workerpb"
	"reduction.dev/reduction/util/size"
)

// maxMessageSize is the maximum size of a single message that can be read.
// Messages larger than this will cause the scanner to return an error.
const maxMessageSize = 64 * size.KB

type SourceReader struct {
	in        io.Reader
	delimiter []byte
}

func NewSourceReader(config SourceConfig) *SourceReader {
	if config.Framing.Delimiter == nil {
		panic("Only delimiter framing is currently supported")
	}
	return &SourceReader{
		in:        os.Stdin,
		delimiter: config.Framing.Delimiter,
	}
}

func (s *SourceReader) ReadEvents() ([][]byte, error) {
	scanner := bufio.NewScanner(s.in)
	buf := make([]byte, maxMessageSize)
	scanner.Buffer(buf, maxMessageSize)

	scanner.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		if i := bytes.Index(data, s.delimiter); i >= 0 {
			return i + len(s.delimiter), data[:i], nil
		}
		if atEOF && len(data) > 0 {
			return len(data), data, nil
		}
		return 0, nil, nil
	})

	var events [][]byte
	for scanner.Scan() {
		record := make([]byte, len(scanner.Bytes()))
		copy(record, scanner.Bytes())
		events = append(events, record)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return events, nil
}

func (s *SourceReader) AssignSplits(splits []*workerpb.SourceSplit) error {
	// No split assignment needed for stdin
	return nil
}

func (s *SourceReader) Checkpoint() [][]byte {
	return nil // No checkpointing needed for stdin
}

var _ connectors.SourceReader = (*SourceReader)(nil)
