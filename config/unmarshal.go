package config

import (
	"fmt"
	"reflect"
	"strconv"

	"google.golang.org/protobuf/encoding/protojson"
	"reduction.dev/reduction-protocol/jobconfigpb"
	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/connectors/embedded"
	"reduction.dev/reduction/connectors/httpapi"
	"reduction.dev/reduction/connectors/kinesis"
	"reduction.dev/reduction/connectors/stdio"
)

// Unmarshal parses a job configuration from JSON format that was marshaled
// using protojson.Marshal(jobconfigpb.JobConfig).
func Unmarshal(data []byte, params *Params) (*Config, error) {
	var pb jobconfigpb.JobConfig
	if err := protojson.Unmarshal(data, &pb); err != nil {
		return nil, fmt.Errorf("invalid config document format: %v", err)
	}

	ResolveVars(reflect.ValueOf(&pb), params)

	// Create the config object from the Job parameters
	config := &Config{
		WorkerCount:              int(pb.Job.WorkerCount.GetValue()),
		KeyGroupCount:            int(pb.Job.KeyGroupCount),
		SavepointStorageLocation: pb.Job.SavepointStorageLocation.GetValue(),
		WorkingStorageLocation:   pb.Job.WorkingStorageLocation.GetValue(),
	}

	// Create sources from the proto messages
	for _, s := range pb.Sources {
		sourceConfig, err := SourceFromProto(s)
		if err != nil {
			return nil, err
		}
		config.Sources = append(config.Sources, sourceConfig)
	}

	// Create sinks from the proto messages
	for _, s := range pb.Sinks {
		sinkConfig, err := sinkFromProto(s)
		if err != nil {
			return nil, err
		}
		config.Sinks = append(config.Sinks, sinkConfig)
	}

	return config, nil
}

func SourceFromProto(source *jobconfigpb.Source) (connectors.SourceConfig, error) {
	switch c := source.Config.(type) {
	case *jobconfigpb.Source_Kinesis:
		return kinesis.SourceConfigFromProto(c.Kinesis), nil
	case *jobconfigpb.Source_HttpApi:
		return httpapi.SourceConfigFromProto(c.HttpApi), nil
	case *jobconfigpb.Source_Embedded:
		return embedded.SourceConfigFromProto(c.Embedded), nil
	case *jobconfigpb.Source_Stdio:
		return stdio.SourceConfigFromProto(c.Stdio), nil
	default:
		return nil, fmt.Errorf("unknown source type %T", source.Config)
	}
}

func sinkFromProto(sink *jobconfigpb.Sink) (connectors.SinkConfig, error) {
	switch c := sink.Config.(type) {
	case *jobconfigpb.Sink_HttpApi:
		return httpapi.SinkConfigFromProto(c.HttpApi), nil
	case *jobconfigpb.Sink_Stdio:
		return stdio.SinkConfigFromProto(c.Stdio), nil
	default:
		return nil, fmt.Errorf("unknown sink type %T", sink.Config)
	}
}

// ResolveVars recursively processes a reflect.Value looking for *Var types
// and resolving the values from the provided params.
func ResolveVars(v reflect.Value, params *Params) error {
	if !v.IsValid() {
		return nil
	}

	// Handle pointer types
	if v.Kind() == reflect.Ptr {
		if v.IsNil() || !v.CanInterface() {
			return nil
		}

		switch concrete := v.Interface().(type) {
		case *jobconfigpb.StringVar:
			if param, ok := concrete.Kind.(*jobconfigpb.StringVar_Param); ok {
				value, found := params.Get(param.Param)
				if !found {
					return fmt.Errorf("parameter %q not found", param.Param)
				}
				concrete.Kind = &jobconfigpb.StringVar_Value{Value: value}
			}
			return nil

		case *jobconfigpb.Int32Var:
			if param, ok := concrete.Kind.(*jobconfigpb.Int32Var_Param); ok {
				value, found := params.Get(param.Param)
				if !found {
					return fmt.Errorf("parameter %q not found", param.Param)
				}
				intValue, err := strconv.ParseInt(value, 10, 32)
				if err != nil {
					return fmt.Errorf("parameter %q is not a valid int32: %v", param.Param, err)
				}
				concrete.Kind = &jobconfigpb.Int32Var_Value{Value: int32(intValue)}
			}
			return nil
		}

		return ResolveVars(v.Elem(), params)
	}

	// Recursively process struct fields and slice elements
	switch v.Kind() {
	case reflect.Struct:
		for i := range v.NumField() {
			if err := ResolveVars(v.Field(i), params); err != nil {
				return err
			}
		}

	case reflect.Slice:
		for i := range v.Len() {
			if err := ResolveVars(v.Index(i), params); err != nil {
				return err
			}
		}
	}

	return nil
}
