package connectors

type SinkWriter interface {
	Write([]byte) error
}
