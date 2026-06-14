package worker

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/thalestmm/gowork/internal/store"
)

// RunStaleJobReaper periodically resets jobs stuck in running status. This
// covers worker crashes and jobs that ignore execution context cancellation.
func RunStaleJobReaper(ctx context.Context, db *store.Queries, interval, staleAfter time.Duration) error {
	if interval <= 0 {
		interval = DefaultReaperInterval
	}
	if staleAfter <= 0 {
		staleAfter = DefaultStaleJobAfter
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			cutoff := time.Now().Add(-staleAfter)
			n, err := db.ReapStaleJobs(ctx, store.ReapStaleJobsParams{
				ErrorMessage: fmt.Sprintf("stale after %s", staleAfter),
				Cutoff:       &cutoff,
			})
			if err != nil {
				return fmt.Errorf("reap stale jobs: %w", err)
			}
			if n > 0 {
				log.Printf("reaped %d stale job(s)", n)
			}
		}
	}
}
