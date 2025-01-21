package sourcerunner

import (
	"context"
	"fmt"

	gproto "google.golang.org/protobuf/proto"
	"reduction.dev/reduction/partitioning"
	"reduction.dev/reduction/proto"
	"reduction.dev/reduction/proto/workerpb"

	"golang.org/x/sync/errgroup"
)

type operatorCluster struct {
	keyGroupCount int
	keySpace      *partitioning.KeySpace
	operators     []proto.Operator
}

type newClusterParams struct {
	keyGroupCount int
	operators     []proto.Operator
}

func newOperatorCluster(params *newClusterParams) *operatorCluster {
	return &operatorCluster{
		keyGroupCount: params.keyGroupCount,
		operators:     params.operators,
		keySpace:      partitioning.NewKeySpace(params.keyGroupCount, len(params.operators)),
	}
}

// routeEvent sends an event to a single operator based on its key.
func (c *operatorCluster) routeEvent(ctx context.Context, key []byte, event *workerpb.Event) error {
	rangeIndex := c.keySpace.RangeIndex(key)
	targetWorker := c.operators[rangeIndex]
	if err := targetWorker.HandleEvent(ctx, event); err != nil {
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
	for _, w := range c.operators {
		g.Go(func() error {
			return w.HandleEvent(gctx, request)
		})
	}
	return g.Wait()
}
