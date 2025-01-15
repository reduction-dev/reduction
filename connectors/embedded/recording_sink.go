package embedded

import "reduction.dev/reduction/connectors"

type RecordingSink struct {
	Values [][]byte
}

func (r *RecordingSink) Write(v []byte) error {
	r.Values = append(r.Values, v)
	return nil
}

var _ connectors.SinkWriter = (*RecordingSink)(nil)
