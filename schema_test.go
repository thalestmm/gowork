package gowork

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSchemaError_Error(t *testing.T) {
	err := &SchemaError{
		Table:          "public.jobs",
		MissingColumns: []string{"id", "slug"},
		ExtraColumns:   []string{"legacy_col"},
		Mismatched: []ColumnMismatch{
			{Column: "priority", Expected: "int4 NOT NULL", Actual: "int8 NOT NULL"},
		},
	}

	msg := err.Error()
	require.Contains(t, msg, `schema mismatch on "public.jobs"`)
	require.Contains(t, msg, "missing columns: id, slug")
	require.Contains(t, msg, "extra columns: legacy_col")
	require.Contains(t, msg, `column "priority": expected int4 NOT NULL, got int8 NOT NULL`)
}

func TestSchemaError_empty(t *testing.T) {
	err := &SchemaError{Table: "public.jobs"}
	require.Contains(t, err.Error(), `schema mismatch on "public.jobs"`)
}

func TestFormatColumn(t *testing.T) {
	require.Equal(t, "uuid NOT NULL", formatColumn("uuid", false))
	require.Equal(t, "jsonb NULL", formatColumn("jsonb", true))
}

func TestColumnNames(t *testing.T) {
	names := columnNames(expectedJobsColumns)
	require.Contains(t, names, "id")
	require.Contains(t, names, "slug")
	require.Len(t, names, len(expectedJobsColumns))
}
