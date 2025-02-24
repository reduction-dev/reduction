package embedded

import (
	"reduction.dev/reduction-protocol/jobconfigpb"
	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/proto/workerpb"
)

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
	config := SourceConfig{
		SplitCount:  int(pb.SplitCount),
		BatchSize:   int(pb.BatchSize),
		GeneratorID: jobconfigpb.EmbeddedSource_GeneratorType_name[int32(pb.Generator)],
	}

	// Set defaults
	if config.SplitCount == 0 {
		config.SplitCount = 1
	}
	if config.BatchSize == 0 {
		config.BatchSize = 1_000
	}

	return config
}

var _ connectors.SourceConfig = SourceConfig{}
