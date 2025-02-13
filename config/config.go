package config

import (
	"errors"
	"fmt"

	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/connectors/embedded"
	"reduction.dev/reduction/connectors/httpapi"
	"reduction.dev/reduction/connectors/kinesis"
	"reduction.dev/reduction/connectors/stdio"
	"reduction.dev/reduction/proto/workerpb"
)

// The object representing Job configuration.
type Config struct {
	WorkerCount              int
	KeyGroupCount            int
	SavepointStorageLocation string
	WorkingStorageLocation   string
	SourceIDs                []string
	Sources                  []connectors.SourceConfig `json:"-"`
	SinkIDs                  []string
	Sinks                    []connectors.SinkConfig `json:"-"`

	// Temporary storage of all concrete source types used to hydrate references
	allSources map[string]connectors.SourceConfig
	// Temporary storage of all concrete sink types used to hydrate references
	allSinks map[string]connectors.SinkConfig
}

func (c *Config) Validate() (err error) {
	for _, s := range c.Sources {
		err = errors.Join(s.Validate())
	}
	if len(c.Sources) != 1 {
		err = errors.Join(fmt.Errorf("need exactly 1 source but had %d", len(c.Sources)))
	}
	if c.WorkingStorageLocation == "" {
		err = errors.Join(fmt.Errorf("WorkingStorageLocation is required"))
	}

	return err
}

func NewSourceReaderFromProto(source *workerpb.Source) connectors.SourceReader {
	switch t := source.Config.(type) {
	case *workerpb.Source_HttpApiConfig:
		return httpapi.NewSourceReader(httpapi.SourceConfig{
			Addr:   t.HttpApiConfig.Addr,
			Topics: t.HttpApiConfig.Topics,
		})
	case *workerpb.Source_KinesisConfig:
		return kinesis.NewSourceReader(kinesis.SourceConfig{
			StreamARN: t.KinesisConfig.StreamArn,
			Endpoint:  t.KinesisConfig.Endpoint,
		})
	case *workerpb.Source_EmbeddedConfig:
		return embedded.NewSourceReader(embedded.SourceConfig{
			SplitCount:  int(t.EmbeddedConfig.SplitCount),
			BatchSize:   int(t.EmbeddedConfig.BatchSize),
			GeneratorID: t.EmbeddedConfig.Generator,
		})
	case *workerpb.Source_StdioConfig:
		return stdio.NewSourceReader(stdio.SourceConfig{
			Framing: stdio.Framing{
				Delimiter: t.StdioConfig.GetFraming().GetDelimited().Delimiter,
			},
		})
	default:
		panic("unknown proto source type")
	}
}

func NewSinkFromProto(sink *workerpb.Sink) connectors.SinkWriter {
	switch t := sink.Config.(type) {
	case *workerpb.Sink_HttpApiConfig:
		return httpapi.NewSink(httpapi.SinkConfig{
			Addr: t.HttpApiConfig.Addr,
		})
	case *workerpb.Sink_StdioConfig:
		return stdio.NewSink(stdio.SinkConfig{})
	default:
		panic("unknown proto sink type")
	}
}
