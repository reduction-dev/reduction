package config_test

import (
	"testing"

	"reduction.dev/reduction-go/jobs"
	cfg "reduction.dev/reduction/config"
	"reduction.dev/reduction/connectors/httpapi"
	"reduction.dev/reduction/connectors/kinesis"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnmarshal(t *testing.T) {
	source := jobs.NewKinesisSource("Source", &jobs.KinesisSourceParams{
		StreamARN: "stream-arn",
		Endpoint:  "http://localhost:12345",
	})
	sink := jobs.NewHTTPAPISink("Sink", &jobs.HTTPAPISinkParams{
		Addr: "http-api-sink-addr",
	})
	job := jobs.NewJob("Job", &jobs.JobParams{
		WorkerCount: 2,
		Sources:     []jobs.Source{source},
		Sinks:       []jobs.Sink{sink},
	})

	defJson := job.Marshal()
	t.Log(string(defJson))
	def, err := cfg.Unmarshal(defJson)
	require.NoError(t, err)

	assert.Equal(t, def.WorkerCount, 2)
	assert.Len(t, def.Sources, 1)
	assert.IsType(t, kinesis.SourceConfig{}, def.Sources[0])

	assert.Len(t, def.Sinks, 1)
	assert.IsType(t, httpapi.SinkConfig{}, def.Sinks[0])
	assert.Equal(t, def.Sinks[0].(httpapi.SinkConfig).Addr, "http-api-sink-addr")
}
