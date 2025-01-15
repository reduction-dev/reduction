package sourcerunner

import (
	"context"
	"fmt"

	gproto "google.golang.org/protobuf/proto"
	"reduction.dev/reduction-handler/handlerpb"
	"reduction.dev/reduction/partitioning"
	"reduction.dev/reduction/proto"
	"reduction.dev/reduction/proto/workerpb"

	"golang.org/x/sync/errgroup"
)

type operatorCluster struct {
	keyGroupCount int
	keySpace      *partitioning.KeySpace
	workers       []proto.Operator
}

type newClusterParams struct {
	keyGroupCount int
	workers       []proto.Operator
}

func newOperatorCluster(params *newClusterParams) *operatorCluster {
	return &operatorCluster{
		keyGroupCount: params.keyGroupCount,
		workers:       params.workers,
		keySpace:      partitioning.NewKeySpace(params.keyGroupCount, len(params.workers)),
	}
}

// routeEvent sends an event to a single operator based on its key.
func (c *operatorCluster) routeEvent(ctx context.Context, event *handlerpb.KeyedEvent) error {
	rangeIndex := c.keySpace.RangeIndex(event.Key)
	targetWorker := c.workers[rangeIndex]
	request := &workerpb.Event{
		Event: &workerpb.Event_KeyedEvent{
			KeyedEvent: event,
		},
	}
	if err := targetWorker.HandleEvent(ctx, request); err != nil {
		return fmt.Errorf("cluster.routeEvent: %v", err)
	}
	return nil
}

// broadcastEvent sends an event to all of the operators.
func (c *operatorCluster) broadcastEvent(ctx context.Context, event gproto.Message) error {
	request, err := proto.PutOneOfEvent(event)
	if err != nil {
		return fmt.Errorf("cluster.broadcastEvent: %v", err)
	}

	g, gctx := errgroup.WithContext(ctx)
	for _, w := range c.workers {
		g.Go(func() error {
			return w.HandleEvent(gctx, request)
		})
	}
	return g.Wait()
}
