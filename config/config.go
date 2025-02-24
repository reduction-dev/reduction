package config

import (
	"errors"
	"fmt"

	"reduction.dev/reduction-protocol/jobconfigpb"
	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/connectors/embedded"
	"reduction.dev/reduction/connectors/httpapi"
	"reduction.dev/reduction/connectors/kinesis"
	"reduction.dev/reduction/connectors/stdio"
)

// The object representing Job configuration.
type Config struct {
	WorkerCount              int
	KeyGroupCount            int
	SavepointStorageLocation string
	WorkingStorageLocation   string
	Sources                  []connectors.SourceConfig
	Sinks                    []connectors.SinkConfig
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

func NewSourceReaderFromProto(source *jobconfigpb.Source) connectors.SourceReader {
	switch t := source.Config.(type) {
	case *jobconfigpb.Source_HttpApi:
		return httpapi.NewSourceReader(httpapi.SourceConfig{
			Addr:   t.HttpApi.Addr,
			Topics: t.HttpApi.Topics,
		})
	case *jobconfigpb.Source_Kinesis:
		return kinesis.NewSourceReader(kinesis.SourceConfig{
			StreamARN: t.Kinesis.StreamArn,
			Endpoint:  t.Kinesis.Endpoint,
		})
	case *jobconfigpb.Source_Embedded:
		return embedded.NewSourceReader(embedded.SourceConfig{
			SplitCount:  int(t.Embedded.SplitCount),
			BatchSize:   int(t.Embedded.BatchSize),
			GeneratorID: t.Embedded.Generator.Enum().String(),
		})
	case *jobconfigpb.Source_Stdio:
		return stdio.NewSourceReader(stdio.SourceConfig{
			Framing: stdio.Framing{
				Delimiter: t.Stdio.Framing.GetDelimited().Delimiter,
			},
		})
	default:
		panic("unknown proto source type")
	}
}

func NewSinkFromProto(sink *jobconfigpb.Sink) connectors.SinkWriter {
	switch t := sink.Config.(type) {
	case *jobconfigpb.Sink_HttpApi:
		return httpapi.NewSink(httpapi.SinkConfig{
			Addr: t.HttpApi.Addr,
		})
	case *jobconfigpb.Sink_Stdio:
		return stdio.NewSink(stdio.SinkConfig{})
	default:
		panic("unknown proto sink type")
	}
}
