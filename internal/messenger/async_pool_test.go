package messenger

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestAsyncPoolLimitsConcurrency(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	pool := newAsyncPool(ctx, asyncWorkerCount, asyncQueueSize)

	var current atomic.Int32
	var maximum atomic.Int32
	release := make(chan struct{})
	var jobs sync.WaitGroup
	jobs.Add(asyncWorkerCount * 2)

	for i := 0; i < asyncWorkerCount*2; i++ {
		if !pool.submit(func() {
			defer jobs.Done()
			running := current.Add(1)
			for {
				observed := maximum.Load()
				if running <= observed || maximum.CompareAndSwap(observed, running) {
					break
				}
			}
			<-release
			current.Add(-1)
		}) {
			t.Fatal("submit rejected before cancellation")
		}
	}

	deadline := time.After(2 * time.Second)
	for maximum.Load() < asyncWorkerCount {
		select {
		case <-deadline:
			t.Fatalf("only %d workers started", maximum.Load())
		default:
			time.Sleep(time.Millisecond)
		}
	}
	close(release)
	jobs.Wait()
	cancel()
	pool.wait()

	if got := maximum.Load(); got != asyncWorkerCount {
		t.Fatalf("maximum concurrency = %d, want %d", got, asyncWorkerCount)
	}
}

func TestAsyncPoolRejectsAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	pool := newAsyncPool(ctx, 1, 1)
	cancel()
	pool.wait()

	if pool.submit(func() {}) {
		t.Fatal("submit accepted after cancellation")
	}
}
