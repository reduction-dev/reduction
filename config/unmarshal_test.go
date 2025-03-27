package config_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clienthttpapi "reduction.dev/reduction-go/connectors/httpapi"
	clientkinesis "reduction.dev/reduction-go/connectors/kinesis"
	"reduction.dev/reduction-go/rxn"
	"reduction.dev/reduction-go/topology"
	cfg "reduction.dev/reduction/config"
	"reduction.dev/reduction/config/jsontemplate"
	"reduction.dev/reduction/connectors/httpapi"
	"reduction.dev/reduction/connectors/kinesis"
)

func TestUnmarshal(t *testing.T) {
	job := &topology.Job{WorkerCount: 2}
	source := clientkinesis.NewSource(job, "Source", &clientkinesis.SourceParams{
		StreamARN: "stream-arn",
		Endpoint:  "http://localhost:12345",
	})
	operator := topology.NewOperator(job, "Operator", &topology.OperatorParams{
		Handler: func(op *topology.Operator) rxn.OperatorHandler {
			return nil
		},
	})
	sink := clienthttpapi.NewSink(job, "Sink", &clienthttpapi.SinkParams{
		Addr: "http-api-sink-addr",
	})
	source.Connect(operator)
	operator.Connect(sink)

	synthesis, err := job.Synthesize()
	require.NoError(t, err)

	t.Log(string(synthesis.Config.Marshal()))
	def, err := cfg.Unmarshal(synthesis.Config.Marshal(), jsontemplate.NewParams())
	require.NoError(t, err)

	assert.Equal(t, def.WorkerCount, 2)
	assert.Len(t, def.Sources, 1)
	assert.IsType(t, kinesis.SourceConfig{}, def.Sources[0])

	assert.Len(t, def.Sinks, 1)
	assert.IsType(t, httpapi.SinkConfig{}, def.Sinks[0])
	assert.Equal(t, def.Sinks[0].(httpapi.SinkConfig).Addr, "http-api-sink-addr")
}

func TestUnmarshalWithParams(t *testing.T) {
	configJSON := `{
		"job": {
			"workerCount": { "$param": "WORKER_COUNT" }
		}
	}`

	params := jsontemplate.NewParams()
	params.Set("WORKER_COUNT", "2")
	def, err := cfg.Unmarshal([]byte(configJSON), params)
	require.NoError(t, err)

	assert.Equal(t, def.WorkerCount, 2)
}
