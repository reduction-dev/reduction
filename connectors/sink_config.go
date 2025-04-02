package connectors

import "reduction.dev/reduction-protocol/jobconfigpb"

type SinkConfig interface {
	Validate() error
	ProtoMessage() *jobconfigpb.Sink
	NewSink() SinkWriter
}
