package bg

import (
	"sync"

	"golang.org/x/sync/errgroup"
)

type AsyncGroup struct {
	eg *errgroup.Group
}

func (g *AsyncGroup) Go(fn func() error) {
	g.eg.Go(fn)
}

// Enqueue a task, serializing functions with the TaskQueue channel.
func (g *AsyncGroup) Enqueue(tq *TaskQueue, fn func() error) {
	tq.c <- fn
	g.eg.Go(func() error {
		tq.mu.Lock()
		fn := <-tq.c
		defer tq.mu.Unlock()
		return fn()
	})
}

func (g *AsyncGroup) Wait() error {
	return g.eg.Wait()
}

func NewAsyncGroup() *AsyncGroup {
	return &AsyncGroup{
		eg: &errgroup.Group{},
	}
}

type TaskQueue struct {
	c  chan func() error
	mu *sync.Mutex
}

func NewQueue(limit int) *TaskQueue {
	return &TaskQueue{
		c:  make(chan func() error, limit),
		mu: &sync.Mutex{},
	}
}
