//go:build integration

package gowork_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	gowork "github.com/thalestmm/gowork"
	"github.com/thalestmm/gowork/internal/testutil"
)

func TestOpen_missingTable(t *testing.T) {
	db := testutil.StartPostgres(t)

	_, err := db.Pool.Exec(context.Background(), "DROP TABLE IF EXISTS jobs")
	require.NoError(t, err)

	_, err = gowork.Open(context.Background(), db.Pool)
	require.Error(t, err)

	schemaErr, ok := err.(*gowork.SchemaError)
	require.True(t, ok)
	require.NotEmpty(t, schemaErr.MissingColumns)
}

func TestOpen_extraColumn(t *testing.T) {
	db := testutil.StartPostgres(t)

	_, err := db.Pool.Exec(context.Background(), "ALTER TABLE jobs ADD COLUMN legacy_col TEXT")
	require.NoError(t, err)

	_, err = gowork.Open(context.Background(), db.Pool)
	require.Error(t, err)

	schemaErr, ok := err.(*gowork.SchemaError)
	require.True(t, ok)
	require.Contains(t, schemaErr.ExtraColumns, "legacy_col")
}

func TestOpen_wrongType(t *testing.T) {
	db := testutil.StartPostgres(t)

	_, err := db.Pool.Exec(context.Background(), "ALTER TABLE jobs ALTER COLUMN priority TYPE BIGINT")
	require.NoError(t, err)

	_, err = gowork.Open(context.Background(), db.Pool)
	require.Error(t, err)

	schemaErr, ok := err.(*gowork.SchemaError)
	require.True(t, ok)
	require.NotEmpty(t, schemaErr.Mismatched)
}

func TestMigrate_idempotent(t *testing.T) {
	db := testutil.StartPostgres(t)

	require.NoError(t, gowork.Migrate(context.Background(), db.Pool))
	require.NoError(t, gowork.Migrate(context.Background(), db.Pool))
}

func TestOpen_withSkipSchemaCheck(t *testing.T) {
	db := testutil.StartPostgres(t)

	_, err := db.Pool.Exec(context.Background(), "DROP TABLE IF EXISTS jobs")
	require.NoError(t, err)

	client, err := gowork.Open(context.Background(), db.Pool, gowork.WithSkipSchemaCheck())
	require.NoError(t, err)
	require.NotNil(t, client)
}
