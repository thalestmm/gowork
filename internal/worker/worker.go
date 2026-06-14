package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/thalestmm/gowork/internal/registry"
	"github.com/thalestmm/gowork/internal/store"
)

type workerDeps struct {
	db         *store.Queries
	poll       time.Duration
	jobTimeout time.Duration
	logger     *slog.Logger
}

type claimFunc func(ctx context.Context, q *store.Queries) (store.Job, error)

func newWorkerDeps(db *store.Queries, cfg Config) workerDeps {
	cfg = cfg.withDefaults()
	return workerDeps{
		db:         db,
		poll:       cfg.Poll,
		jobTimeout: cfg.JobTimeout,
		logger:     cfg.Logger,
	}
}

func (d workerDeps) claim(ctx context.Context, claim claimFunc) (*registry.QueuedJob, bool, error) {
	row, err := claim(ctx, d.db)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}

	queued, err := registry.NewQueuedJob(row)
	if err != nil {
		d.logger.Warn("job rejected",
			"job_id", row.ID,
			"slug", row.Slug,
			"error", err,
		)
		failErr := d.db.FailJob(ctx, store.FailJobParams{
			ErrorMessage: err.Error(),
			ID:           row.ID,
		})
		if failErr != nil {
			return nil, false, fmt.Errorf("build queued job: %w (fail job: %v)", err, failErr)
		}
		return nil, true, nil
	}
	return &queued, true, nil
}

func (d workerDeps) process(ctx context.Context, queued *registry.QueuedJob) error {
	d.logger.Info("job claimed",
		"job_id", queued.ID,
		"slug", queued.Slug,
		"attempt", queued.Attempts,
	)

	_ = d.db.AppendJobLog(ctx, store.AppendJobLogParams{
		LogMessage: fmt.Sprintf("started %q (attempt %d)", queued.Slug, queued.Attempts),
		ID:         queued.ID,
	})

	execCtx, cancel := context.WithTimeout(ctx, d.jobTimeout)
	defer cancel()

	if err := runJob(execCtx, queued.Job); err != nil {
		if failErr := d.db.FailJob(ctx, store.FailJobParams{
			ErrorMessage: err.Error(),
			ID:           queued.ID,
		}); failErr != nil {
			return fmt.Errorf("execute job %s: %w (fail job: %v)", queued.ID, err, failErr)
		}
		d.logger.Warn("job failed",
			"job_id", queued.ID,
			"slug", queued.Slug,
			"attempt", queued.Attempts,
			"error", err,
		)
		_ = d.db.AppendJobLog(ctx, store.AppendJobLogParams{
			LogMessage: fmt.Sprintf("failed: %s", err),
			ID:         queued.ID,
		})
		return nil
	}

	if err := d.db.CompleteJob(ctx, queued.ID); err != nil {
		return fmt.Errorf("complete job %s: %w", queued.ID, err)
	}
	d.logger.Info("job completed",
		"job_id", queued.ID,
		"slug", queued.Slug,
		"attempt", queued.Attempts,
	)
	_ = d.db.AppendJobLog(ctx, store.AppendJobLogParams{
		LogMessage: "completed",
		ID:         queued.ID,
	})
	return nil
}

func runJob(ctx context.Context, job registry.Job) error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- job.Execute(ctx)
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (d workerDeps) runLoop(ctx context.Context, claim claimFunc) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			queued, found, err := d.claim(ctx, claim)
			if err != nil {
				d.logger.Error("claim failed", "error", err)
				return err
			}
			if !found {
				d.logger.Debug("no jobs available",
					"poll_interval", d.poll,
				)
				time.Sleep(d.poll)
				continue
			}
			if queued != nil {
				if err := d.process(ctx, queued); err != nil {
					d.logger.Error("process failed",
						"job_id", queued.ID,
						"slug", queued.Slug,
						"error", err,
					)
					return err
				}
			}
		}
	}
}

type SimpleWorker struct {
	deps workerDeps
}

func NewSimple(db *store.Queries, cfg Config) *SimpleWorker {
	return &SimpleWorker{deps: newWorkerDeps(db, cfg)}
}

func (w *SimpleWorker) Run(ctx context.Context) error {
	return w.deps.runLoop(ctx, func(ctx context.Context, q *store.Queries) (store.Job, error) {
		return q.ClaimNextPendingJob(ctx)
	})
}

type PriorityWorker struct {
	deps        workerDeps
	MinPriority int
	MaxPriority *int
}

func NewPriority(db *store.Queries, cfg Config, minPriority int, maxPriority *int) *PriorityWorker {
	return &PriorityWorker{
		deps:        newWorkerDeps(db, cfg),
		MinPriority: minPriority,
		MaxPriority: maxPriority,
	}
}

func (w *PriorityWorker) Run(ctx context.Context) error {
	minPriority := int32(w.MinPriority)
	var maxPriority *int32
	if w.MaxPriority != nil {
		v := int32(*w.MaxPriority)
		maxPriority = &v
	}

	return w.deps.runLoop(ctx, func(ctx context.Context, q *store.Queries) (store.Job, error) {
		return q.ClaimNextPendingJobByPriority(ctx, store.ClaimNextPendingJobByPriorityParams{
			MinPriority: minPriority,
			MaxPriority: maxPriority,
		})
	})
}

type SpecificTaskWorker struct {
	deps     workerDeps
	TaskSlug string
}

func NewSpecificTask(db *store.Queries, cfg Config, taskSlug string) *SpecificTaskWorker {
	return &SpecificTaskWorker{
		deps:     newWorkerDeps(db, cfg),
		TaskSlug: taskSlug,
	}
}

func (w *SpecificTaskWorker) Run(ctx context.Context) error {
	slug := w.TaskSlug
	return w.deps.runLoop(ctx, func(ctx context.Context, q *store.Queries) (store.Job, error) {
		return q.ClaimNextPendingJobBySlug(ctx, slug)
	})
}
