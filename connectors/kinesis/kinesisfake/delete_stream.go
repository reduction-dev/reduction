package kinesisfake

import (
	"encoding/json"
)

type DeleteStreamRequest struct {
	StreamName string
}

type DeleteStreamResponse struct{}

func (f *Fake) deleteStream(body []byte) (*DeleteStreamResponse, error) {
	var request DeleteStreamRequest
	err := json.Unmarshal(body, &request)
	if err != nil {
		return nil, err
	}

	delete(f.db.streams, request.StreamName)

	return &DeleteStreamResponse{}, nil
}
