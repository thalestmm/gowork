package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	gowork "github.com/thalestmm/gowork"
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

	interval := envDuration("CLIENT_ENQUEUE_INTERVAL", 5*time.Second)
	limit := envInt("CLIENT_ENQUEUE_LIMIT", 0)

	log.Printf("starting client enqueue_interval=%s enqueue_limit=%d", interval, limit)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	count := 0
	for {
		if err := enqueuePing(ctx, client, count+1); err != nil {
			log.Printf("enqueue failed: %v", err)
		} else {
			count++
			if limit > 0 && count >= limit {
				log.Printf("reached enqueue limit (%d), stopping", limit)
				return
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func enqueuePing(ctx context.Context, client *gowork.Client, n int) error {
	payload, err := json.Marshal(map[string]string{
		"message": fmt.Sprintf("ping #%d at %s", n, time.Now().Format(time.RFC3339)),
	})
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

	log.Printf("enqueued ping job id=%s", id)
	return nil
}

func envInt(key string, fallback int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
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
