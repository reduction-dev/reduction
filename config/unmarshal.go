package config

import (
	"encoding/json"
	"fmt"

	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/connectors/embedded"
	"reduction.dev/reduction/connectors/httpapi"
	"reduction.dev/reduction/connectors/kinesis"
	"reduction.dev/reduction/connectors/stdio"
	"reduction.dev/reduction/util/sliceu"
)

// Unmarshal the format produced by gflow/jobs. This decoding is done in the
// core module rather than in gflow/jobs because gflow/jobs is planned to be
// implemented in several languages.
func Unmarshal(data []byte) (*Config, error) {
	// Unmarshal as constructs of unknown types according to the shape of the JSON
	// input.
	var doc struct {
		Job     construct
		Sources map[string]construct
		Sinks   map[string]construct
	}
	err := json.Unmarshal(data, &doc)
	if err != nil {
		return nil, fmt.Errorf("invalid config document format: %v", err)
	}

	// Unmarshal the top level Config object which corresponds to "Job" in the
	// JSON.
	var config Config
	if err := json.Unmarshal(doc.Job.Params, &config); err != nil {
		return nil, fmt.Errorf("job params invalid: %v, %v", err, string(doc.Job.Params))
	}

	// Parse each source construct into concrete types.
	config.allSources = make(map[string]connectors.SourceConfig, len(doc.Sources))
	for id, s := range doc.Sources {
		var err error
		if config.allSources[id], err = sourceFromConstruct(s); err != nil {
			return nil, err
		}
	}

	// Resolve the job references to sources
	config.Sources = sliceu.Map(config.SourceIDs, func(id string) connectors.SourceConfig {
		return config.allSources[id]
	})

	// Parse each sink construct into concrete types.
	config.allSinks = make(map[string]connectors.SinkConfig, len(doc.Sinks))
	for id, s := range doc.Sinks {
		var err error
		if config.allSinks[id], err = sinkFromConstruct(s); err != nil {
			return nil, err
		}
	}

	// Resolve the job references to Sinks
	config.Sinks = sliceu.Map(config.SinkIDs, func(id string) connectors.SinkConfig {
		return config.allSinks[id]
	})

	return &config, nil
}

type construct struct {
	Type   string
	Params json.RawMessage
}

func sourceFromConstruct(cons construct) (connectors.SourceConfig, error) {
	switch cons.Type {
	case "Source:Kinesis":
		return kinesis.ParseSourceConfig(cons.Params)
	case "Source:HTTPAPI":
		return httpapi.ParseSourceConfig(cons.Params)
	case "Source:Embedded":
		return embedded.ParseSourceConfig(cons.Params)
	case "Source:Stdio":
		return stdio.ParseSourceConfig(cons.Params)
	default:
		return nil, fmt.Errorf("unknown source type %s", cons.Type)
	}
}

func sinkFromConstruct(cons construct) (connectors.SinkConfig, error) {
	switch cons.Type {
	case "Sink:HTTPAPI":
		var config httpapi.SinkConfig
		if err := json.Unmarshal(cons.Params, &config); err != nil {
			return nil, err
		}
		return config, nil
	case "Sink:Stdio":
		return &stdio.SinkConfig{}, nil
	default:
		return nil, fmt.Errorf("unknown sink type %s", cons.Type)
	}
}
