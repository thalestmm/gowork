package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	gowork "github.com/thalestmm/gowork"
	"github.com/thalestmm/gowork/internal/logging"
	_ "github.com/thalestmm/gowork/examples/ping"
)

func main() {
	logger := logging.Setup("worker")

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgres@localhost:5432/gowork?sslmode=disable"
	}

	pool, err := pgxpool.New(context.Background(), dbURL)
	if err != nil {
		logger.Error("database connection failed", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := gowork.Migrate(ctx, pool); err != nil {
		logger.Error("migrate failed", "error", err)
		os.Exit(1)
	}

	client, err := gowork.Open(ctx, pool)
	if err != nil {
		logger.Error("open failed", "error", err)
		os.Exit(1)
	}

	cfg := configFromEnv(logger)
	concurrency := envInt(logger, "WORKER_CONCURRENCY", 4)

	go func() {
		if err := client.RunStaleJobReaper(ctx, cfg.ReaperInterval, cfg.StaleJobAfter); err != nil && err != context.Canceled {
			logger.Error("stale job reaper stopped", "error", err)
			stop()
		}
	}()

	worker := client.NewSimpleWorker(gowork.WorkerConfig{
		Poll:       cfg.Poll,
		JobTimeout: cfg.JobTimeout,
		Logger:     logger.With("subsystem", "worker"),
	})

	logger.Info("worker started",
		"concurrency", concurrency,
		"job_timeout", cfg.JobTimeout,
		"stale_after", cfg.StaleJobAfter,
		"poll_interval", cfg.Poll,
	)

	if err := gowork.RunConcurrent(ctx, concurrency, worker); err != nil {
		logger.Error("workers stopped", "error", err)
		os.Exit(1)
	}
}

type runtimeConfig struct {
	Poll           time.Duration
	JobTimeout     time.Duration
	StaleJobAfter  time.Duration
	ReaperInterval time.Duration
}

func configFromEnv(logger *slog.Logger) runtimeConfig {
	return runtimeConfig{
		Poll:           envDuration(logger, "WORKER_POLL_INTERVAL", gowork.DefaultPollInterval),
		JobTimeout:     envDuration(logger, "JOB_TIMEOUT", gowork.DefaultJobTimeout),
		StaleJobAfter:  envDuration(logger, "STALE_JOB_TIMEOUT", gowork.DefaultStaleJobAfter),
		ReaperInterval: envDuration(logger, "REAPER_INTERVAL", gowork.DefaultReaperInterval),
	}
}

func envInt(logger *slog.Logger, key string, fallback int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		logger.Warn("invalid env var, using default",
			"key", key,
			"value", raw,
			"default", fallback,
		)
		return fallback
	}
	return n
}

func envDuration(logger *slog.Logger, key string, fallback time.Duration) time.Duration {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		logger.Warn("invalid env var, using default",
			"key", key,
			"value", raw,
			"default", fallback,
		)
		return fallback
	}
	return d
}
