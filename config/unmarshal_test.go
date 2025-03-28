package config_test

import (
	"testing"

	"reflect"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clienthttpapi "reduction.dev/reduction-go/connectors/httpapi"
	clientkinesis "reduction.dev/reduction-go/connectors/kinesis"
	"reduction.dev/reduction-go/rxn"
	"reduction.dev/reduction-go/topology"
	"reduction.dev/reduction-protocol/jobconfigpb"
	"reduction.dev/reduction/config"
	cfg "reduction.dev/reduction/config"
	"reduction.dev/reduction/connectors/httpapi"
	"reduction.dev/reduction/connectors/kinesis"
)

func TestUnmarshal(t *testing.T) {
	job := &topology.Job{WorkerCount: topology.IntValue(2)}
	source := clientkinesis.NewSource(job, "Source", &clientkinesis.SourceParams{
		StreamARN: topology.StringValue("stream-arn"),
		Endpoint:  topology.StringValue("http://localhost:12345"),
	})
	operator := topology.NewOperator(job, "Operator", &topology.OperatorParams{
		Handler: func(op *topology.Operator) rxn.OperatorHandler {
			return nil
		},
	})
	sink := clienthttpapi.NewSink(job, "Sink", &clienthttpapi.SinkParams{
		Addr: topology.StringValue("http-api-sink-addr"),
	})
	source.Connect(operator)
	operator.Connect(sink)

	synthesis, err := job.Synthesize()
	require.NoError(t, err)

	t.Log(string(synthesis.Config.Marshal()))
	def, err := cfg.Unmarshal(synthesis.Config.Marshal(), config.NewParams())
	require.NoError(t, err)

	assert.Equal(t, def.WorkerCount, 2)
	assert.Len(t, def.Sources, 1)
	assert.IsType(t, kinesis.SourceConfig{}, def.Sources[0])

	assert.Len(t, def.Sinks, 1)
	assert.IsType(t, httpapi.SinkConfig{}, def.Sinks[0])
	assert.Equal(t, def.Sinks[0].(httpapi.SinkConfig).Addr, "http-api-sink-addr")
}

func TestResolveVars_StringVar(t *testing.T) {
	params := config.NewParams()
	params.Set("string_param", "hello world")
	sv := &jobconfigpb.StringVar{
		Kind: &jobconfigpb.StringVar_Param{Param: "string_param"},
	}

	err := cfg.ResolveVars(reflect.ValueOf(sv), params)
	require.NoError(t, err)

	assert.Equal(t, "hello world", sv.GetValue())
}

func TestResolveVars_Int32Var(t *testing.T) {
	params := config.NewParams()
	params.Set("int_param", "42")
	iv := &jobconfigpb.Int32Var{
		Kind: &jobconfigpb.Int32Var_Param{Param: "int_param"},
	}

	err := cfg.ResolveVars(reflect.ValueOf(iv), params)
	require.NoError(t, err)

	assert.Equal(t, int32(42), iv.GetValue())
}

func TestResolveVars_MissingParam(t *testing.T) {
	params := config.NewParams()
	sv := &jobconfigpb.StringVar{
		Kind: &jobconfigpb.StringVar_Param{Param: "missing_param"},
	}

	err := cfg.ResolveVars(reflect.ValueOf(sv), params)

	assert.ErrorContains(t, err, "parameter \"missing_param\" not found")
}

func TestResolveVars_InvalidInt(t *testing.T) {
	params := config.NewParams()
	params.Set("int_param", "not a number")
	iv := &jobconfigpb.Int32Var{
		Kind: &jobconfigpb.Int32Var_Param{Param: "int_param"},
	}

	err := cfg.ResolveVars(reflect.ValueOf(iv), params)

	assert.ErrorContains(t, err, "not a valid int32")
}

func TestResolveVars_NestedStruct(t *testing.T) {
	params := config.NewParams()
	params.Set("string_param", "hello world")

	// Create a test struct with nested variables
	type TestStruct struct {
		NestedStruct struct {
			StringField2 *jobconfigpb.StringVar
		}
		StringSlice []*jobconfigpb.StringVar
	}

	ts := TestStruct{}
	ts.NestedStruct.StringField2 = &jobconfigpb.StringVar{
		Kind: &jobconfigpb.StringVar_Param{Param: "string_param"},
	}
	ts.StringSlice = []*jobconfigpb.StringVar{
		{Kind: &jobconfigpb.StringVar_Param{Param: "string_param"}},
	}

	err := cfg.ResolveVars(reflect.ValueOf(&ts), params)

	require.NoError(t, err)
	assert.Equal(t, "hello world", ts.NestedStruct.StringField2.GetValue())
	assert.Equal(t, "hello world", ts.StringSlice[0].GetValue())
}
