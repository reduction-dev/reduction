package connectors

import (
	"reduction.dev/reduction-protocol/jobconfigpb"
)

type SourceConfig interface {
	Validate() error
	NewSourceSplitter() SourceSplitter
	NewSourceReader() SourceReader
	ProtoMessage() *jobconfigpb.Source
}
