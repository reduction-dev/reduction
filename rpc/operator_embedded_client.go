package rpc

import (
	"context"

	"reduction.dev/reduction/proto"
	"reduction.dev/reduction/proto/workerpb"
	"reduction.dev/reduction/workers/operator"
)

// OperatorEmbeddedClient directly invokes methods on an operator.
type OperatorEmbeddedClient struct {
	op       *operator.Operator
	senderID string
	host     string
	id       string
}

type NewOperatorEmbeddedClientParams struct {
	Operator *operator.Operator
	SenderID string
	Host     string
	ID       string
}

func NewOperatorEmbeddedClient(params NewOperatorEmbeddedClientParams) *OperatorEmbeddedClient {
	return &OperatorEmbeddedClient{
		op:       params.Operator,
		senderID: params.SenderID,
		host:     params.Host,
		id:       params.ID,
	}
}

func (c *OperatorEmbeddedClient) HandleEventBatch(ctx context.Context, batch []*workerpb.Event) error {
	for _, e := range batch {
		if err := c.op.HandleEvent(ctx, c.senderID, e); err != nil {
			return err
		}
	}

	return nil
}

func (c *OperatorEmbeddedClient) Host() string {
	return c.host
}

func (c *OperatorEmbeddedClient) ID() string {
	return c.id
}

func (c *OperatorEmbeddedClient) Start(ctx context.Context, req *workerpb.StartOperatorRequest) error {
	// Sink parameter is nil because this code is currently used in testrun, where
	// the caller provides an in-memory sink for testing.
	return c.op.HandleStart(ctx, req, nil)
}

func (c *OperatorEmbeddedClient) UpdateRetainedCheckpoints(ctx context.Context, ids []uint64) error {
	panic("unimplemented")
}

// NeedsTable implements proto.Operator.
func (c *OperatorEmbeddedClient) NeedsTable(ctx context.Context, fileURI string) (bool, error) {
	panic("unimplemented")
}

var _ proto.Operator = &OperatorEmbeddedClient{}
