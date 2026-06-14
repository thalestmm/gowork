package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/thalestmm/gowork/internal/store"
)

// RunStaleJobReaper periodically resets jobs stuck in running status. This
// covers worker crashes and jobs that ignore execution context cancellation.
func RunStaleJobReaper(ctx context.Context, db *store.Queries, interval, staleAfter time.Duration) error {
	logger := slog.Default().With("subsystem", "reaper")

	if interval <= 0 {
		interval = DefaultReaperInterval
	}
	if staleAfter <= 0 {
		staleAfter = DefaultStaleJobAfter
	}

	logger.Info("reaper started",
		"interval", interval,
		"stale_after", staleAfter,
	)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("reaper stopped")
			return ctx.Err()
		case <-ticker.C:
			cutoff := time.Now().Add(-staleAfter)
			n, err := db.ReapStaleJobs(ctx, store.ReapStaleJobsParams{
				ErrorMessage: fmt.Sprintf("stale after %s", staleAfter),
				Cutoff:       &cutoff,
			})
			if err != nil {
				logger.Error("reap failed", "error", err)
				return fmt.Errorf("reap stale jobs: %w", err)
			}
			if n > 0 {
				logger.Info("reaped stale jobs", "count", n)
			}
		}
	}
}
