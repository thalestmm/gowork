package gowork

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

const jobsTable = "jobs"

type columnSpec struct {
	Name     string
	UDTName  string
	Nullable bool
}

var expectedJobsColumns = []columnSpec{
	{Name: "id", UDTName: "uuid", Nullable: false},
	{Name: "slug", UDTName: "text", Nullable: false},
	{Name: "payload", UDTName: "jsonb", Nullable: true},
	{Name: "priority", UDTName: "int4", Nullable: false},
	{Name: "max_attempts", UDTName: "int4", Nullable: true},
	{Name: "status", UDTName: "text", Nullable: false},
	{Name: "attempts", UDTName: "int4", Nullable: false},
	{Name: "logs", UDTName: "_text", Nullable: true},
	{Name: "errors", UDTName: "_text", Nullable: true},
	{Name: "created_at", UDTName: "timestamptz", Nullable: false},
	{Name: "started_at", UDTName: "timestamptz", Nullable: true},
	{Name: "ended_at", UDTName: "timestamptz", Nullable: true},
}

type ColumnMismatch struct {
	Column   string
	Expected string
	Actual   string
}

type SchemaError struct {
	Table          string
	MissingColumns []string
	ExtraColumns   []string
	Mismatched     []ColumnMismatch
}

func (e *SchemaError) Error() string {
	var parts []string
	parts = append(parts, fmt.Sprintf("gowork: jobs table schema mismatch on %q", e.Table))
	if len(e.MissingColumns) > 0 {
		parts = append(parts, fmt.Sprintf("missing columns: %s", strings.Join(e.MissingColumns, ", ")))
	}
	if len(e.ExtraColumns) > 0 {
		parts = append(parts, fmt.Sprintf("extra columns: %s", strings.Join(e.ExtraColumns, ", ")))
	}
	for _, m := range e.Mismatched {
		parts = append(parts, fmt.Sprintf("column %q: expected %s, got %s", m.Column, m.Expected, m.Actual))
	}
	return strings.Join(parts, "; ")
}

type actualColumn struct {
	Name     string
	UDTName  string
	Nullable bool
}

func jobsTableExists(ctx context.Context, pool *pgxpool.Pool) (bool, error) {
	var exists bool
	err := pool.QueryRow(ctx, `
SELECT EXISTS (
  SELECT 1 FROM information_schema.tables
  WHERE table_schema = 'public' AND table_name = $1
)`, jobsTable).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check jobs table: %w", err)
	}
	return exists, nil
}

// ValidateSchema checks that public.jobs exists and matches the expected schema exactly.
func ValidateSchema(ctx context.Context, pool *pgxpool.Pool) error {
	rows, err := pool.Query(ctx, `
SELECT column_name, udt_name, is_nullable
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = $1
ORDER BY ordinal_position`, jobsTable)
	if err != nil {
		return fmt.Errorf("validate schema: %w", err)
	}
	defer rows.Close()

	actual := make(map[string]actualColumn)
	for rows.Next() {
		var col actualColumn
		var nullable string
		if err := rows.Scan(&col.Name, &col.UDTName, &nullable); err != nil {
			return fmt.Errorf("validate schema: scan: %w", err)
		}
		col.Nullable = nullable == "YES"
		actual[col.Name] = col
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("validate schema: %w", err)
	}

	if len(actual) == 0 {
		return &SchemaError{Table: "public." + jobsTable, MissingColumns: columnNames(expectedJobsColumns)}
	}

	expected := make(map[string]columnSpec, len(expectedJobsColumns))
	for _, spec := range expectedJobsColumns {
		expected[spec.Name] = spec
	}

	schemaErr := &SchemaError{Table: "public." + jobsTable}

	for name, spec := range expected {
		got, ok := actual[name]
		if !ok {
			schemaErr.MissingColumns = append(schemaErr.MissingColumns, name)
			continue
		}
		if got.UDTName != spec.UDTName || got.Nullable != spec.Nullable {
			schemaErr.Mismatched = append(schemaErr.Mismatched, ColumnMismatch{
				Column:   name,
				Expected: formatColumn(spec.UDTName, spec.Nullable),
				Actual:   formatColumn(got.UDTName, got.Nullable),
			})
		}
	}

	for name := range actual {
		if _, ok := expected[name]; !ok {
			schemaErr.ExtraColumns = append(schemaErr.ExtraColumns, name)
		}
	}

	sort.Strings(schemaErr.MissingColumns)
	sort.Strings(schemaErr.ExtraColumns)
	sort.Slice(schemaErr.Mismatched, func(i, j int) bool {
		return schemaErr.Mismatched[i].Column < schemaErr.Mismatched[j].Column
	})

	if len(schemaErr.MissingColumns) > 0 || len(schemaErr.ExtraColumns) > 0 || len(schemaErr.Mismatched) > 0 {
		return schemaErr
	}
	return nil
}

func columnNames(specs []columnSpec) []string {
	names := make([]string, len(specs))
	for i, spec := range specs {
		names[i] = spec.Name
	}
	sort.Strings(names)
	return names
}

func formatColumn(udtName string, nullable bool) string {
	null := "NOT NULL"
	if nullable {
		null = "NULL"
	}
	return udtName + " " + null
}
