package config

import (
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"
	"reduction.dev/reduction-protocol/jobconfigpb"
	"reduction.dev/reduction/config/jsontemplate"
	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/connectors/embedded"
	"reduction.dev/reduction/connectors/httpapi"
	"reduction.dev/reduction/connectors/kinesis"
	"reduction.dev/reduction/connectors/stdio"
)

// Unmarshal parses a job configuration from JSON format that was marshaled
// using protojson.Marshal(jobconfigpb.JobConfig).
func Unmarshal(data []byte, params *jsontemplate.Params) (*Config, error) {
	resolvedJSON, err := jsontemplate.Resolve(data, &jobconfigpb.JobConfig{}, params)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve parameters: %w", err)
	}

	var pb jobconfigpb.JobConfig
	if err := protojson.Unmarshal(resolvedJSON, &pb); err != nil {
		return nil, fmt.Errorf("invalid config document format: %v", err)
	}

	// Create the config object from the Job parameters
	config := &Config{
		WorkerCount:              int(pb.Job.WorkerCount),
		KeyGroupCount:            int(pb.Job.KeyGroupCount),
		SavepointStorageLocation: pb.Job.SavepointStorageLocation,
		WorkingStorageLocation:   pb.Job.WorkingStorageLocation,
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
