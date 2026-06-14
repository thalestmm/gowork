package gowork

import (
	"context"
	"time"

	intworker "github.com/thalestmm/gowork/internal/worker"
)

const (
	DefaultPollInterval   = intworker.DefaultPollInterval
	DefaultJobTimeout     = intworker.DefaultJobTimeout
	DefaultStaleJobAfter  = intworker.DefaultStaleJobAfter
	DefaultReaperInterval = intworker.DefaultReaperInterval
)

type WorkerConfig = intworker.Config

type Worker = intworker.Worker

func RunConcurrent(ctx context.Context, concurrency int, workers ...Worker) error {
	return intworker.RunConcurrent(ctx, concurrency, workers...)
}

func (c *Client) NewSimpleWorker(cfg WorkerConfig) Worker {
	return intworker.NewSimple(c.q, cfg)
}

func (c *Client) NewPriorityWorker(cfg WorkerConfig, minPriority int, maxPriority *int) Worker {
	return intworker.NewPriority(c.q, cfg, minPriority, maxPriority)
}

func (c *Client) NewSpecificTaskWorker(cfg WorkerConfig, taskSlug string) Worker {
	return intworker.NewSpecificTask(c.q, cfg, taskSlug)
}

func (c *Client) RunStaleJobReaper(ctx context.Context, interval, staleAfter time.Duration) error {
	return intworker.RunStaleJobReaper(ctx, c.q, interval, staleAfter)
}
