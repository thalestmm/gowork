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
	_ "github.com/thalestmm/gowork/jobs" // register job handlers
	"github.com/thalestmm/gowork/internal/worker"
	"github.com/thalestmm/gowork/repository"
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

	queries := repository.New(pool)
	cfg := configFromEnv()
	concurrency := envInt("WORKER_CONCURRENCY", 4)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		if err := worker.RunStaleJobReaper(ctx, queries, cfg.ReaperInterval, cfg.StaleJobAfter); err != nil && err != context.Canceled {
			log.Printf("stale job reaper stopped: %v", err)
			stop()
		}
	}()

	w := worker.NewSimple(queries, worker.Config{
		Poll:       cfg.Poll,
		JobTimeout: cfg.JobTimeout,
	})

	log.Printf("starting worker concurrency=%d job_timeout=%s stale_after=%s",
		concurrency, cfg.JobTimeout, cfg.StaleJobAfter)
	if err := worker.RunConcurrent(ctx, concurrency, w); err != nil {
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
		Poll:           envDuration("WORKER_POLL_INTERVAL", worker.DefaultPollInterval),
		JobTimeout:     envDuration("JOB_TIMEOUT", worker.DefaultJobTimeout),
		StaleJobAfter:  envDuration("STALE_JOB_TIMEOUT", worker.DefaultStaleJobAfter),
		ReaperInterval: envDuration("REAPER_INTERVAL", worker.DefaultReaperInterval),
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
