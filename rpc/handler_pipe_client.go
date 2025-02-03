package rpc

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"

	"google.golang.org/protobuf/proto"
	"reduction.dev/reduction-handler/handlerpb"
	rxnproto "reduction.dev/reduction/proto"
)

const (
	methodKeyEventBatch     byte = 0x01
	methodProcessEventBatch byte = 0x02
)

// HandlerPipeClient communicates with a handler over stdin/stdout. The protocol is:
//
// Send a message:
//
//	<4 bytes: uint32 length of message><1 byte: method><proto message>
//
// Receive a response:
//
//	<4 bytes: uint32 length of message><proto message>
type HandlerPipeClient struct {
	input  io.Reader
	output io.Writer
}

func NewHandlerPipeClient(input io.Reader, output io.Writer) *HandlerPipeClient {
	return &HandlerPipeClient{input: input, output: output}
}

func (h *HandlerPipeClient) ProcessEventBatch(ctx context.Context, req *handlerpb.ProcessEventBatchRequest) (*handlerpb.ProcessEventBatchResponse, error) {
	if err := h.writeMessage(methodProcessEventBatch, req); err != nil {
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
	if err := h.writeMessage(methodKeyEventBatch, req); err != nil {
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

// writeMessage writes a method byte and a protobuf parameter to output.
func (h *HandlerPipeClient) writeMessage(methodType byte, msg proto.Message) error {
	data, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	// 1 byte for message type
	msgLen := 1 + len(data)

	// Write message length
	if err := binary.Write(h.output, binary.BigEndian, uint32(msgLen)); err != nil {
		return fmt.Errorf("failed to write message length: %w", err)
	}

	// Write method byte
	if _, err := h.output.Write([]byte{methodType}); err != nil {
		return fmt.Errorf("failed to write method type: %w", err)
	}

	// Write parameter
	if _, err := h.output.Write(data); err != nil {
		return fmt.Errorf("failed to write message data: %w", err)
	}

	return nil
}

// readMessage reads a protobuf return value from input.
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
