package messenger

import (
	"context"
	"sync"
)

const (
	asyncWorkerCount = 8
	asyncQueueSize   = 128
)

// asyncPool bounds expensive background work while keeping short bursts away
// from network and WebRTC callback goroutines.
type asyncPool struct {
	ctx  context.Context
	jobs chan func()
	wg   sync.WaitGroup
}

func newAsyncPool(ctx context.Context, workers, queueSize int) *asyncPool {
	p := &asyncPool{
		ctx:  ctx,
		jobs: make(chan func(), queueSize),
	}
	p.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go p.worker()
	}
	return p
}

func (p *asyncPool) submit(job func()) bool {
	if job == nil || p.ctx.Err() != nil {
		return false
	}
	select {
	case p.jobs <- job:
		return true
	case <-p.ctx.Done():
		return false
	}
}

func (p *asyncPool) trySubmit(job func()) bool {
	if job == nil || p.ctx.Err() != nil {
		return false
	}
	select {
	case p.jobs <- job:
		return true
	default:
		return false
	}
}

func (p *asyncPool) worker() {
	defer p.wg.Done()
	for {
		select {
		case <-p.ctx.Done():
			return
		case job := <-p.jobs:
			if p.ctx.Err() != nil {
				return
			}
			job()
		}
	}
}

func (p *asyncPool) wait() {
	p.wg.Wait()
}
