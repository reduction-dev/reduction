package stdio

import (
	"reduction.dev/reduction-protocol/jobconfigpb"
	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/proto/workerpb"
)

type SourceConfig struct {
	Framing Framing
}

type Framing struct {
	Delimiter     []byte
	LengthEncoded bool
}

func (s *SourceConfig) IsSourceConfig() {}

func (s *SourceConfig) ProtoMessage() *workerpb.Source {
	return &workerpb.Source{
		Config: &workerpb.Source_StdioConfig{
			StdioConfig: &workerpb.Source_Stdio{
				Framing: &workerpb.StdioFraming{
					Config: &workerpb.StdioFraming_Delimited_{
						Delimited: &workerpb.StdioFraming_Delimited{
							Delimiter: s.Framing.Delimiter,
						},
					},
				},
			},
		},
	}
}

func (s *SourceConfig) Validate() error {
	return nil
}

func (s *SourceConfig) NewSourceSplitter() connectors.SourceSplitter {
	return &SourceSplitter{}
}

func SourceConfigFromProto(pb *jobconfigpb.StdioSource) *SourceConfig {
	return &SourceConfig{
		Framing: Framing{
			Delimiter: pb.Framing.Delimiter,
		},
	}
}

var _ connectors.SourceConfig = (*SourceConfig)(nil)
