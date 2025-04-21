package stdio

import (
	"reduction.dev/reduction-protocol/jobconfigpb"
	"reduction.dev/reduction/connectors"
)

type SourceConfig struct {
	Framing Framing
}

type Framing struct {
	Delimiter     []byte
	LengthEncoded bool
}

func (s SourceConfig) ProtoMessage() *jobconfigpb.Source {
	return &jobconfigpb.Source{
		Config: &jobconfigpb.Source_Stdio{
			Stdio: &jobconfigpb.StdioSource{
				Framing: &jobconfigpb.Framing{
					Scheme: &jobconfigpb.Framing_Delimited{
						Delimited: &jobconfigpb.Framing_DelimitedScheme{
							Delimiter: s.Framing.Delimiter,
						},
					},
				},
			},
		},
	}
}

func (s SourceConfig) Validate() error {
	return nil
}

func (s SourceConfig) NewSourceSplitter(sourceRunnerIDs []string, hooks connectors.SourceSplitterHooks, errChan chan<- error) connectors.SourceSplitter {
	return &SourceSplitter{}
}

func (s SourceConfig) NewSourceReader() connectors.SourceReader {
	return NewSourceReader(s)
}

func SourceConfigFromProto(pb *jobconfigpb.StdioSource) *SourceConfig {
	return &SourceConfig{
		Framing: Framing{
			Delimiter: pb.Framing.GetDelimited().Delimiter,
		},
	}
}

var _ connectors.SourceConfig = (*SourceConfig)(nil)
