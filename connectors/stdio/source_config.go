package stdio

import (
	"encoding/json"

	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/proto/workerpb"
)

type SourceConfig struct {
	Framing Framing
}

// ParseSourceConfig parses and validates a stdio source configuration
func ParseSourceConfig(data []byte) (*SourceConfig, error) {
	var config *SourceConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return &SourceConfig{}, err
	}
	return config, nil
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

var _ connectors.SourceConfig = (*SourceConfig)(nil)
