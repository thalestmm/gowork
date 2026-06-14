package worker

import (
	"context"
	"errors"
	"sync"
)

type Worker interface {
	Run(ctx context.Context) error
}

// RunConcurrent starts each worker in its own goroutine pool. The caller owns
// lifecycle (context cancellation, concurrency config); workers stay blocking
// claim loops so they remain easy to test.
func RunConcurrent(ctx context.Context, concurrency int, workers ...Worker) error {
	if concurrency <= 0 {
		concurrency = 1
	}
	if len(workers) == 0 {
		return nil
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)

	for _, worker := range workers {
		for range concurrency {
			wg.Add(1)
			go func(w Worker) {
				defer wg.Done()
				if err := w.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
					mu.Lock()
					errs = append(errs, err)
					mu.Unlock()
					cancel()
				}
			}(worker)
		}
	}

	wg.Wait()
	return errors.Join(errs...)
}
