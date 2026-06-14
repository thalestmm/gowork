package testutil

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/thalestmm/gowork/internal/store"
)

func migrateTestDB(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS gowork`); err != nil {
		return fmt.Errorf("migrate test db: create schema: %w", err)
	}

	if _, err := pool.Exec(ctx, `
CREATE TABLE IF NOT EXISTS gowork.schema_migrations (
  version INT PRIMARY KEY,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)`); err != nil {
		return fmt.Errorf("migrate test db: create version table: %w", err)
	}

	var exists bool
	err := pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM gowork.schema_migrations WHERE version = $1)`,
		1,
	).Scan(&exists)
	if err != nil {
		return fmt.Errorf("migrate test db: check version: %w", err)
	}
	if exists {
		return nil
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("migrate test db: begin: %w", err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, store.JobsMigrationSQL); err != nil {
		return fmt.Errorf("migrate test db: apply jobs table: %w", err)
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO gowork.schema_migrations (version) VALUES ($1)`,
		1,
	); err != nil {
		return fmt.Errorf("migrate test db: record version: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("migrate test db: commit: %w", err)
	}
	return nil
}
