package embedded

import (
	"reduction.dev/reduction-protocol/jobconfigpb"
	"reduction.dev/reduction/connectors"
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

func (s SourceConfig) NewSourceSplitter(sourceRunnerIDs []string, hooks connectors.SourceSplitterHooks, errChan chan<- error) connectors.SourceSplitter {
	return NewSourceSplitter(s, sourceRunnerIDs, hooks)
}

func (s SourceConfig) NewSourceReader(hooks connectors.SourceReaderHooks) connectors.SourceReader {
	return NewSourceReader(s)
}

func (s SourceConfig) ProtoMessage() *jobconfigpb.Source {
	generatorEnumValue := jobconfigpb.EmbeddedSource_GeneratorType_value[s.GeneratorID]
	return &jobconfigpb.Source{
		Config: &jobconfigpb.Source_Embedded{
			Embedded: &jobconfigpb.EmbeddedSource{
				SplitCount: int32(s.SplitCount),
				BatchSize:  int32(s.BatchSize),
				Generator:  jobconfigpb.EmbeddedSource_GeneratorType(generatorEnumValue),
			},
		},
	}
}

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
