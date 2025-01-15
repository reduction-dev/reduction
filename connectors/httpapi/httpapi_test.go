package httpapi_test

import (
	"encoding/json"
	"testing"

	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/proto/workerpb"

	"reduction.dev/reduction/connectors/httpapi"
	"reduction.dev/reduction/connectors/httpapi/httpapitest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadWriteBatchOfEvents(t *testing.T) {
	svr := httpapitest.StartServer()
	defer svr.Close()

	sink := httpapi.NewSink(httpapi.SinkConfig{
		Addr: svr.URL(),
	})

	// These test events are opaque to the sink.
	eventsToWrite := [][]byte{
		[]byte("d1"),
		[]byte("d2"),
		[]byte("d3"),
	}

	// Write the expected JSON object to the sink. Each sink receives raw bytes
	// and converts the data into their expected request type so that the
	// interface for sink.Write is uniform.
	for _, e := range eventsToWrite {
		sinkEventBytes, err := json.Marshal(&httpapi.HTTPSinkEvent{
			Topic: "topic-a",
			Data:  e,
		})
		require.NoError(t, err)
		err = sink.Write(sinkEventBytes)
		require.NoError(t, err)
	}

	source := httpapi.NewSourceReader(httpapi.SourceConfig{
		Addr:   svr.URL(),
		Topics: []string{"topic-a"},
	})
	source.SetSplits([]*workerpb.SourceSplit{{
		SplitId:  "only",
		SourceId: "tbd",
	}})

	// In this test we read all the data at once, resulting in EOI on first call.
	readEvents, err := source.ReadEvents()
	require.ErrorIs(t, err, connectors.ErrEndOfInput)

	assert.Equal(t, eventsToWrite, readEvents)
}

func TestReadWriteSingleEvents(t *testing.T) {
	svr := httpapitest.StartServer(httpapitest.WithReadBatchSize(1))
	defer svr.Close()

	sink := httpapi.NewSink(httpapi.SinkConfig{
		Addr: svr.URL(),
	})

	// These test events are opaque to the sink.
	eventsToWrite := [][]byte{
		[]byte("d1"),
		[]byte("d2"),
		[]byte("d3"),
	}

	// Write the expected JSON object to the sink. Each sink receives raw bytes
	// and converts the data into their expected request type so that the
	// interface for sink.Write is uniform.
	for _, e := range eventsToWrite {
		sinkEventBytes, err := json.Marshal(&httpapi.HTTPSinkEvent{
			Topic: "topic-a",
			Data:  e,
		})
		require.NoError(t, err)
		err = sink.Write(sinkEventBytes)
		require.NoError(t, err)
	}

	source := httpapi.NewSourceReader(httpapi.SourceConfig{
		Addr:   svr.URL(),
		Topics: []string{"topic-a"},
	})
	source.SetSplits([]*workerpb.SourceSplit{{
		SplitId:  "only",
		SourceId: "tbd",
	}})

	// Read events in a loop relying on the cursor state of the source to advance.
	var readEvents [][]byte
	for {
		resp, err := source.ReadEvents()
		require.Len(t, resp, 1, "server respects batch size 1")
		readEvents = append(readEvents, resp...)
		if err == connectors.ErrEndOfInput {
			break
		}
		require.NoError(t, err)
	}

	assert.Equal(t, eventsToWrite, readEvents)
}
