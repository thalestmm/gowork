package gowork

// Package gowork is a Postgres-backed job queue. Consumers must run Migrate
// separately before Open to create the jobs table, or apply equivalent DDL
// via their own migration tool.
//
// Open validates that public.jobs matches the expected schema exactly and
// never runs migrations.
