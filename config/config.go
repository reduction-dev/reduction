package config

import (
	"errors"
	"fmt"

	"reduction.dev/reduction-protocol/jobconfigpb"
	"reduction.dev/reduction/connectors"
	"reduction.dev/reduction/connectors/httpapi"
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

func NewSinkFromProto(sink *jobconfigpb.Sink) connectors.SinkWriter {
	switch t := sink.Config.(type) {
	case *jobconfigpb.Sink_HttpApi:
		return httpapi.SinkConfigFromProto(t.HttpApi).NewSink()
	case *jobconfigpb.Sink_Stdio:
		return stdio.SinkConfigFromProto(t.Stdio).NewSink()
	default:
		panic("unknown proto sink type")
	}
}
