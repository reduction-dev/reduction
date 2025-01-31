package sourcerunner

import (
	"context"
	"fmt"

	gproto "google.golang.org/protobuf/proto"
	"reduction.dev/reduction/batching"
	"reduction.dev/reduction/partitioning"
	"reduction.dev/reduction/proto"
	"reduction.dev/reduction/proto/workerpb"
)

type operatorCluster struct {
	keyGroupCount int
	keySpace      *partitioning.KeySpace
	operators     []*batchingOperator
}

type newClusterParams struct {
	keyGroupCount  int
	operators      []proto.Operator
	batchingParams batching.EventBatcherParams
	errChan        chan<- error
}

func newOperatorCluster(ctx context.Context, params *newClusterParams) *operatorCluster {
	operators := make([]*batchingOperator, len(params.operators))
	for i, op := range params.operators {
		operators[i] = newBatchingOperator(ctx, op, params.batchingParams, params.errChan)
	}
	return &operatorCluster{
		keyGroupCount: params.keyGroupCount,
		operators:     operators,
		keySpace:      partitioning.NewKeySpace(params.keyGroupCount, len(params.operators)),
	}
}

// routeEvent sends an event to a single operator based on its key.
func (c *operatorCluster) routeEvent(key []byte, event *workerpb.Event) {
	rangeIndex := c.keySpace.RangeIndex(key)
	c.operators[rangeIndex].HandleEvent(event)
}

// broadcastEvent sends an event to all of the operators.
func (c *operatorCluster) broadcastEvent(event gproto.Message) error {
	request, err := proto.PutOneOfEvent(event)
	if err != nil {
		return fmt.Errorf("cluster.broadcastEvent: %v", err)
	}

	for _, op := range c.operators {
		op.HandleEvent(request)
	}
	return nil
}

func (c *operatorCluster) flush() {
	for _, op := range c.operators {
		op.Flush()
	}
}

type batchingOperator struct {
	op      proto.Operator
	batcher *batching.EventBatcher[*workerpb.Event]
	errChan chan<- error
	batches chan []*workerpb.Event
}

func newBatchingOperator(ctx context.Context, op proto.Operator, params batching.EventBatcherParams, errChan chan<- error) *batchingOperator {
	o := &batchingOperator{
		op:      op,
		batcher: batching.NewEventBatcher[*workerpb.Event](ctx, params),
		batches: make(chan []*workerpb.Event),
		errChan: errChan,
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case batchToken := <-o.batcher.BatchTimedOut:
				if err := o.op.HandleEventBatch(ctx, o.batcher.Flush(batchToken)); err != nil {
					o.errChan <- err
				}
			case batch := <-o.batches:
				if err := o.op.HandleEventBatch(ctx, batch); err != nil {
					o.errChan <- err
				}
			}
		}
	}()

	return o
}

func (o *batchingOperator) HandleEvent(event *workerpb.Event) {
	o.batcher.Add(event)
	if o.batcher.IsFull() {
		o.Flush()
	}
}

func (o *batchingOperator) Flush() {
	o.batches <- o.batcher.Flush(batching.CurrentBatch)
}
