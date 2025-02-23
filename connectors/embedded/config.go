package embedded

import (
	"encoding/json"

	"reduction.dev/reduction-protocol/jobconfigpb"
	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/proto/workerpb"
)

// ParseSourceConfig parses and sets defaults for an embedded source configuration
func ParseSourceConfig(data []byte) (SourceConfig, error) {
	var config SourceConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return SourceConfig{}, err
	}

	// Set defaults
	if config.SplitCount == 0 {
		config.SplitCount = 1
	}
	if config.BatchSize == 0 {
		config.BatchSize = 1_000
	}

	return config, nil
}

// SourceConfig contains configuration for the embedded source connector
type SourceConfig struct {
	SplitCount  int
	BatchSize   int
	GeneratorID string
}

func (s SourceConfig) Validate() error {
	return nil
}

func (s SourceConfig) NewSourceSplitter() connectors.SourceSplitter {
	return NewSourceSplitter(s)
}

func (s SourceConfig) ProtoMessage() *workerpb.Source {
	return &workerpb.Source{
		Config: &workerpb.Source_EmbeddedConfig{
			EmbeddedConfig: &workerpb.Source_Embedded{
				SplitCount: int32(s.SplitCount),
				BatchSize:  int32(s.BatchSize),
				Generator:  s.GeneratorID,
			},
		},
	}
}

func (s SourceConfig) IsSourceConfig() {}

func SourceConfigFromProto(pb *jobconfigpb.EmbeddedSource) SourceConfig {
	return SourceConfig{
		SplitCount:  int(pb.SplitCount),
		BatchSize:   int(pb.BatchSize),
		GeneratorID: jobconfigpb.EmbeddedSource_GeneratorType_name[int32(pb.Generator)],
	}
}

var _ connectors.SourceConfig = SourceConfig{}
