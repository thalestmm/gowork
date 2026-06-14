package store

import _ "embed"

// JobsMigrationSQL is the DDL for public.jobs. Used by gowork.Migrate and test helpers.
//
//go:embed migrations/001_jobs.sql
var JobsMigrationSQL string
