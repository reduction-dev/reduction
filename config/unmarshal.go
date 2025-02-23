package config

import (
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"
	"reduction.dev/reduction-protocol/jobconfigpb"
	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/connectors/embedded"
	"reduction.dev/reduction/connectors/httpapi"
	"reduction.dev/reduction/connectors/kinesis"
	"reduction.dev/reduction/connectors/stdio"
	"reduction.dev/reduction/util/sliceu"
)

// Unmarshal parses a job configuration from JSON format that was marshaled using
// protojson.Marshal(jobconfigpb.JobConfig)
func Unmarshal(data []byte) (*Config, error) {
	var pb jobconfigpb.JobConfig
	if err := protojson.Unmarshal(data, &pb); err != nil {
		return nil, fmt.Errorf("invalid config document format: %v", err)
	}

	// Create the config object from the Job parameters
	config := &Config{
		WorkerCount:              int(pb.Job.WorkerCount),
		KeyGroupCount:            int(pb.Job.KeyGroupCount),
		SavepointStorageLocation: pb.Job.SavepointStorageLocation,
		WorkingStorageLocation:   pb.Job.WorkingStorageLocation,
	}

	// Parse each source into concrete types
	config.allSources = make(map[string]connectors.SourceConfig, len(pb.Sources))
	for _, s := range pb.Sources {
		var err error
		if config.allSources[s.Id], err = sourceFromProto(s); err != nil {
			return nil, err
		}
		config.SourceIDs = append(config.SourceIDs, s.Id)
	}

	// Create the source list in the same order as SourceIDs
	config.Sources = sliceu.Map(config.SourceIDs, func(id string) connectors.SourceConfig {
		return config.allSources[id]
	})

	// Parse each sink into concrete types
	config.allSinks = make(map[string]connectors.SinkConfig, len(pb.Sinks))
	for _, s := range pb.Sinks {
		var err error
		if config.allSinks[s.Id], err = sinkFromProto(s); err != nil {
			return nil, err
		}
		config.SinkIDs = append(config.SinkIDs, s.Id)
	}

	// Create the sink list in the same order as SinkIDs
	config.Sinks = sliceu.Map(config.SinkIDs, func(id string) connectors.SinkConfig {
		return config.allSinks[id]
	})

	return config, nil
}

func sourceFromProto(source *jobconfigpb.Source) (connectors.SourceConfig, error) {
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
