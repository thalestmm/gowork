package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	gowork "github.com/thalestmm/gowork"
	_ "github.com/thalestmm/gowork/examples/ping"
)

func main() {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgres@localhost:5432/gowork?sslmode=disable"
	}

	pool, err := pgxpool.New(context.Background(), dbURL)
	if err != nil {
		log.Fatalf("connect to database: %v", err)
	}
	defer pool.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := gowork.Migrate(ctx, pool); err != nil {
		log.Fatalf("migrate: %v", err)
	}

	client, err := gowork.Open(ctx, pool)
	if err != nil {
		log.Fatalf("open: %v", err)
	}

	cfg := configFromEnv()
	concurrency := envInt("WORKER_CONCURRENCY", 4)

	go func() {
		if err := client.RunStaleJobReaper(ctx, cfg.ReaperInterval, cfg.StaleJobAfter); err != nil && err != context.Canceled {
			log.Printf("stale job reaper stopped: %v", err)
			stop()
		}
	}()

	worker := client.NewSimpleWorker(gowork.WorkerConfig{
		Poll:       cfg.Poll,
		JobTimeout: cfg.JobTimeout,
	})

	log.Printf("starting worker concurrency=%d job_timeout=%s stale_after=%s",
		concurrency, cfg.JobTimeout, cfg.StaleJobAfter)
	if err := gowork.RunConcurrent(ctx, concurrency, worker); err != nil {
		log.Fatalf("workers: %v", err)
	}
}

type runtimeConfig struct {
	Poll           time.Duration
	JobTimeout     time.Duration
	StaleJobAfter  time.Duration
	ReaperInterval time.Duration
}

func configFromEnv() runtimeConfig {
	return runtimeConfig{
		Poll:           envDuration("WORKER_POLL_INTERVAL", gowork.DefaultPollInterval),
		JobTimeout:     envDuration("JOB_TIMEOUT", gowork.DefaultJobTimeout),
		StaleJobAfter:  envDuration("STALE_JOB_TIMEOUT", gowork.DefaultStaleJobAfter),
		ReaperInterval: envDuration("REAPER_INTERVAL", gowork.DefaultReaperInterval),
	}
}

func envInt(key string, fallback int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		log.Printf("invalid %s=%q, using %d", key, raw, fallback)
		return fallback
	}
	return n
}

func envDuration(key string, fallback time.Duration) time.Duration {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		log.Printf("invalid %s=%q, using %s", key, raw, fallback)
		return fallback
	}
	return d
}
