package testrun

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"time"

	"golang.org/x/sync/errgroup"
	"reduction.dev/reduction/batching"
	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/proto"
	"reduction.dev/reduction/proto/jobpb"
	"reduction.dev/reduction/proto/workerpb"
	"reduction.dev/reduction/rpc"
	"reduction.dev/reduction/workers/operator"
	"reduction.dev/reduction/workers/sourcerunner"
)

var batchingParams = batching.EventBatcherParams{
	MaxDelay: 10 * time.Millisecond,
	MaxSize:  100,
}

func Run(input io.Reader, output io.Writer) error {
	// First phase: collect all events
	reader := bufio.NewReader(os.Stdin)
	events, err := readEvents(reader)
	if err != nil {
		return fmt.Errorf("failed to read events: %w", err)
	}

	// Second phase: process events through sourcerunner
	op := operator.NewOperator(operator.NewOperatorParams{
		ID:          "testrun",
		Host:        "testrun",
		Job:         &proto.NoopJob{},
		UserHandler: rpc.NewHandlerPipeClient(input, output),
	})
	sr := sourcerunner.New(sourcerunner.NewParams{
		Host:        "testrun",
		UserHandler: rpc.NewHandlerPipeClient(input, output),
		Job:         &proto.NoopJob{},
		OperatorFactory: func(senderID string, node *jobpb.NodeIdentity, errChan chan<- error) proto.Operator {
			return rpc.NewOperatorEmbeddedClient(rpc.NewOperatorEmbeddedClientParams{
				Operator: op,
				SenderID: senderID,
				Host:     node.Host,
				ID:       node.Id,
			})
		},
		SourceReaderFactory: func(s *workerpb.Source) connectors.SourceReader {
			return &fixtureSourceReader{events}
		},
		EventBatching: batchingParams,
	})

	eg, ctx := errgroup.WithContext(context.Background())
	ctx, cancel := context.WithCancel(ctx)
	eg.Go(func() error {
		return sr.Start(ctx)
	})
	eg.Go(func() error {
		defer cancel()
		return op.Start(ctx) // Will end on EOI
	})

	err = op.HandleStart(context.Background(), &workerpb.StartOperatorRequest{
		OperatorIds:     []string{"testrun"},
		SourceRunnerIds: []string{sr.ID},
		KeyGroupCount:   1,
		Sinks:           []*workerpb.Sink{},
		StorageLocation: "memory:///storage",
	}, nil)
	if err != nil {
		return fmt.Errorf("failed to start operator: %w", err)
	}

	err = sr.HandleStart(context.Background(), &workerpb.StartSourceRunnerRequest{
		KeyGroupCount: 1,
		Splits: []*workerpb.SourceSplit{{
			SplitId:  "testrun",
			SourceId: "testrun",
		}},
		Operators: []*jobpb.NodeIdentity{{Id: "testrun", Host: "testrun"}},

		Sources: []*workerpb.Source{{Config: &workerpb.Source_EmbeddedConfig{
			EmbeddedConfig: &workerpb.Source_Embedded{},
		}}},
	})
	if err != nil {
		return fmt.Errorf("failed to start source runner: %w", err)
	}

	return eg.Wait()
}

// Read events from reader until reaching zero-length message.
func readEvents(reader *bufio.Reader) ([][]byte, error) {
	var events [][]byte

	for {
		var length uint32
		if err := binary.Read(reader, binary.BigEndian, &length); err != nil {
			if err == io.EOF {
				return nil, fmt.Errorf("unexpected EOF before delimiter")
			}
			return nil, fmt.Errorf("failed to read message length: %w", err)
		}

		// Zero-length message signals end of input
		if length == 0 {
			return events, nil
		}

		data := make([]byte, length)
		if _, err := io.ReadFull(reader, data); err != nil {
			return nil, fmt.Errorf("failed to read message data: %w", err)
		}

		events = append(events, data)
	}
}
