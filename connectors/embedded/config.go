package embedded

import (
	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/proto/workerpb"
)

type SourceConfig struct {
	SplitCount int
	BatchSize  int
	Generator  string
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
				Generator:  s.Generator,
			},
		},
	}
}

func (s SourceConfig) IsSourceConfig() {}

var _ connectors.SourceConfig = SourceConfig{}
