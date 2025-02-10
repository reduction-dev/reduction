package rpc

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"

	"google.golang.org/protobuf/proto"
	"reduction.dev/reduction-protocol/handlerpb"
	"reduction.dev/reduction-protocol/testrunpb"
	rxnproto "reduction.dev/reduction/proto"
)

type HandlerPipeClient struct {
	input  io.Reader
	output io.Writer
}

func NewHandlerPipeClient(input io.Reader, output io.Writer) *HandlerPipeClient {
	return &HandlerPipeClient{input: input, output: output}
}

func (h *HandlerPipeClient) ProcessEventBatch(ctx context.Context, req *handlerpb.ProcessEventBatchRequest) (*handlerpb.ProcessEventBatchResponse, error) {
	cmd := &testrunpb.HandlerCommand{
		Command: &testrunpb.HandlerCommand_ProcessEventBatch{
			ProcessEventBatch: &testrunpb.ProcessEventBatch{
				ProcessEventBatchRequest: req,
			},
		},
	}

	if err := h.writeMessage(cmd); err != nil {
		return nil, err
	}

	resp := &handlerpb.ProcessEventBatchResponse{}
	if err := h.readMessage(resp); err != nil {
		return nil, err
	}

	return resp, nil
}

func (h *HandlerPipeClient) KeyEventBatch(ctx context.Context, events [][]byte) ([][]*handlerpb.KeyedEvent, error) {
	req := &handlerpb.KeyEventBatchRequest{Values: events}
	cmd := &testrunpb.HandlerCommand{
		Command: &testrunpb.HandlerCommand_KeyEventBatch{
			KeyEventBatch: &testrunpb.KeyEventBatch{
				KeyEventBatchRequest: req,
			},
		},
	}

	if err := h.writeMessage(cmd); err != nil {
		return nil, err
	}

	resp := &handlerpb.KeyEventBatchResponse{}
	if err := h.readMessage(resp); err != nil {
		return nil, err
	}

	keyedEvents := make([][]*handlerpb.KeyedEvent, len(resp.Results))
	for i, result := range resp.Results {
		keyedEvents[i] = result.Events
	}

	return keyedEvents, nil
}

func (h *HandlerPipeClient) writeMessage(msg proto.Message) error {
	data, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	if err := binary.Write(h.output, binary.BigEndian, uint32(len(data))); err != nil {
		return fmt.Errorf("failed to write message length: %w", err)
	}

	if _, err := h.output.Write(data); err != nil {
		return fmt.Errorf("failed to write message data: %w", err)
	}

	return nil
}

func (h *HandlerPipeClient) readMessage(msg proto.Message) error {
	// Read message length
	var length uint32
	if err := binary.Read(h.input, binary.BigEndian, &length); err != nil {
		return fmt.Errorf("failed to read message length: %w", err)
	}

	// Read message data
	data := make([]byte, length)
	if _, err := io.ReadFull(h.input, data); err != nil {
		return fmt.Errorf("failed to read message data: %w", err)
	}
	if err := proto.Unmarshal(data, msg); err != nil {
		return fmt.Errorf("failed to unmarshal message: %w", err)
	}

	return nil
}

var _ rxnproto.Handler = (*HandlerPipeClient)(nil)
