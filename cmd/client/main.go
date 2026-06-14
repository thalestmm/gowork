package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	gowork "github.com/thalestmm/gowork"
	"github.com/thalestmm/gowork/internal/logging"
)

func main() {
	logger := logging.Setup("client")

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

	interval := envDuration(logger, "CLIENT_ENQUEUE_INTERVAL", 5*time.Second)
	limit := envInt(logger, "CLIENT_ENQUEUE_LIMIT", 0)

	logger.Info("client started",
		"enqueue_interval", interval,
		"enqueue_limit", limit,
	)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	count := 0
	for {
		if err := enqueuePing(ctx, logger, client, count+1); err != nil {
			logger.Error("enqueue failed", "error", err)
		} else {
			count++
			if limit > 0 && count >= limit {
				logger.Info("enqueue limit reached", "limit", limit)
				return
			}
		}

		select {
		case <-ctx.Done():
			logger.Info("client stopped")
			return
		case <-ticker.C:
		}
	}
}

func enqueuePing(ctx context.Context, logger *slog.Logger, client *gowork.Client, n int) error {
	message := fmt.Sprintf("ping #%d at %s", n, time.Now().Format(time.RFC3339))
	payload, err := json.Marshal(map[string]string{"message": message})
	if err != nil {
		return err
	}

	id, err := client.Enqueue(ctx, gowork.EnqueueOpts{
		Slug:    "ping",
		Payload: payload,
	})
	if err != nil {
		return err
	}

	logger.Info("job enqueued", "job_id", id, "slug", "ping", "message", message)
	return nil
}

func envInt(logger *slog.Logger, key string, fallback int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
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
