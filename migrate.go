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
// If public.jobs already exists, Migrate validates that it matches the expected
// schema exactly and returns a *SchemaError on mismatch. Open performs the
// same validation but never creates or alters the table.
//
// Migration state is tracked in gowork.schema_migrations and does not
// interfere with goose, golang-migrate, or other consumer migration tools.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	if err := ensureGoworkSchema(ctx, pool); err != nil {
		return err
	}

	exists, err := jobsTableExists(ctx, pool)
	if err != nil {
		return err
	}

	if exists {
		if err := ValidateSchema(ctx, pool); err != nil {
			return err
		}
		return recordMigrationVersion(ctx, pool)
	}

	versionApplied, err := migrationVersionApplied(ctx, pool)
	if err != nil {
		return err
	}
	if versionApplied {
		return fmt.Errorf("migrate: version %d recorded but public.jobs is missing", migrationVersion)
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

func ensureGoworkSchema(ctx context.Context, pool *pgxpool.Pool) error {
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
	return nil
}

func migrationVersionApplied(ctx context.Context, pool *pgxpool.Pool) (bool, error) {
	var exists bool
	err := pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM gowork.schema_migrations WHERE version = $1)`,
		migrationVersion,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("migrate: check version: %w", err)
	}
	return exists, nil
}

func recordMigrationVersion(ctx context.Context, pool *pgxpool.Pool) error {
	applied, err := migrationVersionApplied(ctx, pool)
	if err != nil {
		return err
	}
	if applied {
		return nil
	}

	if _, err := pool.Exec(ctx,
		`INSERT INTO gowork.schema_migrations (version) VALUES ($1)`,
		migrationVersion,
	); err != nil {
		return fmt.Errorf("migrate: record version: %w", err)
	}
	return nil
}
