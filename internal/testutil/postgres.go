package testutil

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/thalestmm/gowork/repository"
)

type DB struct {
	Pool    *pgxpool.Pool
	Queries *repository.Queries
}

type InsertOpts struct {
	ID          uuid.UUID
	Slug        string
	Payload     json.RawMessage
	Priority    int32
	MaxAttempts *int32
}

func StartPostgres(t *testing.T) *DB {
	t.Helper()

	ctx := context.Background()
	container, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("gowork_test"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, testcontainers.TerminateContainer(container))
	})

	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	pool, err := pgxpool.New(ctx, connStr)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	migrate(t, pool)

	return &DB{
		Pool:    pool,
		Queries: repository.New(pool),
	}
}

func migrate(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(context.Background(), `
CREATE TABLE IF NOT EXISTS jobs (
  id UUID PRIMARY KEY,
  slug TEXT NOT NULL,
  payload JSONB,
  priority INT NOT NULL DEFAULT 0,
  max_attempts INT,
  status TEXT NOT NULL DEFAULT 'pending',
  attempts INT NOT NULL DEFAULT 0,
  logs TEXT[],
  errors TEXT[],
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  started_at TIMESTAMPTZ,
  ended_at TIMESTAMPTZ
)`)
	require.NoError(t, err)
}

func TruncateJobs(t *testing.T, db *DB) {
	t.Helper()
	_, err := db.Pool.Exec(context.Background(), "TRUNCATE jobs")
	require.NoError(t, err)
}

func InsertJob(t *testing.T, db *DB, opts InsertOpts) repository.Job {
	t.Helper()

	if opts.ID == uuid.Nil {
		opts.ID = uuid.New()
	}
	if opts.Slug == "" {
		opts.Slug = SlugSuccess
	}

	var payload *json.RawMessage
	if len(opts.Payload) > 0 {
		p := opts.Payload
		payload = &p
	}

	job, err := db.Queries.InsertJob(context.Background(), repository.InsertJobParams{
		ID:          opts.ID,
		Slug:        opts.Slug,
		Payload:     payload,
		Priority:    opts.Priority,
		MaxAttempts: opts.MaxAttempts,
	})
	require.NoError(t, err)
	return job
}

func GetJob(t *testing.T, db *DB, id uuid.UUID) repository.Job {
	t.Helper()

	var job repository.Job
	err := db.Pool.QueryRow(context.Background(), `
SELECT id, slug, payload, priority, max_attempts, status, attempts, logs, errors, created_at, started_at, ended_at
FROM jobs WHERE id = $1`, id).Scan(
		&job.ID,
		&job.Slug,
		&job.Payload,
		&job.Priority,
		&job.MaxAttempts,
		&job.Status,
		&job.Attempts,
		&job.Logs,
		&job.Errors,
		&job.CreatedAt,
		&job.StartedAt,
		&job.EndedAt,
	)
	require.NoError(t, err)
	return job
}

func SetJobStartedAt(t *testing.T, db *DB, id uuid.UUID, at time.Time) {
	t.Helper()
	_, err := db.Pool.Exec(context.Background(),
		`UPDATE jobs SET started_at = $2 WHERE id = $1`, id, at)
	require.NoError(t, err)
}

func SetJobCreatedAt(t *testing.T, db *DB, id uuid.UUID, at time.Time) {
	t.Helper()
	_, err := db.Pool.Exec(context.Background(),
		`UPDATE jobs SET created_at = $2 WHERE id = $1`, id, at)
	require.NoError(t, err)
}
