package gowork

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/thalestmm/gowork/internal/store"
)

const migrationVersion = 1

// Migrate creates the gowork schema version table and applies pending DDL.
// This is a separate setup step — Open never calls Migrate.
//
// Migration state is tracked in gowork.schema_migrations and does not
// interfere with goose, golang-migrate, or other consumer migration tools.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS gowork`); err != nil {
		return fmt.Errorf("migrate: create schema: %w", err)
	}

	if _, err := pool.Exec(ctx, `
CREATE TABLE IF NOT EXISTS gowork.schema_migrations (
  version INT PRIMARY KEY,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)`); err != nil {
		return fmt.Errorf("migrate: create version table: %w", err)
	}

	var exists bool
	err := pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM gowork.schema_migrations WHERE version = $1)`,
		migrationVersion,
	).Scan(&exists)
	if err != nil {
		return fmt.Errorf("migrate: check version: %w", err)
	}
	if exists {
		return nil
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("migrate: begin: %w", err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, store.JobsMigrationSQL); err != nil {
		return fmt.Errorf("migrate: apply jobs table: %w", err)
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO gowork.schema_migrations (version) VALUES ($1)`,
		migrationVersion,
	); err != nil {
		return fmt.Errorf("migrate: record version: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("migrate: commit: %w", err)
	}
	return nil
}
