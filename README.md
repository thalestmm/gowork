# gowork

A Postgres-backed job queue library for Go. Jobs are stored as rows in a `jobs` table, claimed atomically by workers using `FOR UPDATE SKIP LOCKED`, and executed by Go structs registered at startup via a slug-based registry.

## Getting started

```go
import (
    "context"
    "encoding/json"

    "github.com/jackc/pgx/v5/pgxpool"
    gowork "github.com/thalestmm/gowork"
    _ "your/module/jobs" // blank-import packages that register handlers
)

pool, _ := pgxpool.New(ctx, dbURL)

// Migrate is a separate setup step — Open never runs migrations.
if err := gowork.Migrate(ctx, pool); err != nil {
    log.Fatal(err)
}

client, err := gowork.Open(ctx, pool)
if err != nil {
    // *gowork.SchemaError when public.jobs doesn't match the expected schema
    log.Fatal(err)
}

payload, _ := json.Marshal(map[string]string{"message": "hello"})
id, err := client.Enqueue(ctx, gowork.EnqueueOpts{
    Slug:    "ping",
    Payload: payload,
})

worker := client.NewSimpleWorker(gowork.WorkerConfig{})
go client.RunStaleJobReaper(ctx, gowork.DefaultReaperInterval, gowork.DefaultStaleJobAfter)
gowork.RunConcurrent(ctx, 4, worker)
```

### Schema requirements

Consumers must have a `public.jobs` table that matches gowork's expected schema exactly. Two options:

1. **Use `gowork.Migrate`** — creates the table when missing, validates an existing table against the expected schema (returns `*SchemaError` on mismatch), and tracks versions in `gowork.schema_migrations` (isolated from goose, golang-migrate, etc.).
2. **Apply your own migrations** — copy the DDL from [`internal/store/migrations/001_jobs.sql`](internal/store/migrations/001_jobs.sql) into your migration tool.

`gowork.Open` validates the schema and returns a `*SchemaError` on mismatch (missing/extra columns or wrong types). It never migrates. For tests only, `gowork.WithSkipSchemaCheck()` disables validation.

## How it works

### Overview

```
Enqueue (client.Enqueue)     Worker claim loop
       │                            │
       ▼                            ▼
  ┌─────────┐    FOR UPDATE    ┌──────────┐
  │  jobs   │ ◄──SKIP LOCKED── │  Worker  │
  │  table  │                  │  pool    │
  └─────────┘                  └──────────┘
       │                            │
       │                     slug → Job.Execute(ctx)
       │                            │
       └──── CompleteJob / FailJob ◄┘
```

Workers claim a DB row, look up a Go handler by `slug`, parse the JSON payload, and call `Execute`. The public `gowork.Job` interface is the handler contract; the DB row type stays internal.

### Job lifecycle

```
pending ──claim──► running ──success──► completed
   ▲                  │
   │                  └──failure──► pending (retry)
   │                              └──max attempts──► failed
   │
   └── stale reaper (worker crash / hung job)
```

1. **Enqueue** — a row is inserted with `status = pending`.
2. **Claim** — a worker atomically selects the next eligible row, sets `status = running`, increments `attempts`, and sets `started_at`.
3. **Execute** — the registered handler runs with a timeout context.
4. **Complete** — on success, `status = completed` and `ended_at` is set.
5. **Fail** — on error, the message is appended to `errors`. If `attempts >= max_attempts`, the job becomes `failed`; otherwise it returns to `pending` for retry.
6. **Reap** — a background reaper resets jobs stuck in `running` longer than `STALE_JOB_TIMEOUT`.

### Claiming and concurrency

Claim queries use a single `UPDATE ... WHERE id = (SELECT ... FOR UPDATE SKIP LOCKED)` statement. This means:

- Multiple worker goroutines or processes can run safely in parallel.
- Two workers will never claim the same row.
- When the queue is empty, workers sleep for `WORKER_POLL_INTERVAL` before trying again.

Jobs are ordered by **priority descending**, then **created_at ascending** (highest priority, oldest first).

### Worker types

Three worker implementations share the same claim → execute → complete/fail loop but differ in which jobs they pick up:

| Worker | Constructor | Picks up |
|--------|-------------|----------|
| Simple | `client.NewSimpleWorker(cfg)` | Next pending job (any slug) |
| Priority | `client.NewPriorityWorker(cfg, min, max)` | Next pending job where `min < priority < max` (exclusive bounds; nil max = no upper bound) |
| Specific task | `client.NewSpecificTaskWorker(cfg, slug)` | Next pending job matching a single slug |

The worker binary owns orchestration: it configures concurrency, starts goroutines via `gowork.RunConcurrent`, and runs the stale-job reaper in the background. Each worker's `Run(ctx)` is a blocking loop, which keeps the logic easy to test.

### Timeouts

Two complementary timeouts protect the queue:

| Timeout | Env var | Default | Purpose |
|---------|---------|---------|---------|
| Execution | `JOB_TIMEOUT` | `5m` | Max time a single `Execute(ctx)` may run |
| Stale job | `STALE_JOB_TIMEOUT` | `10m` | Reclaim jobs stuck in `running` (crashed worker, ignored context) |
| Reaper interval | `REAPER_INTERVAL` | `1m` | How often the reaper checks for stale jobs |

Set `STALE_JOB_TIMEOUT` greater than `JOB_TIMEOUT` so slow jobs fail via execution timeout before the reaper intervenes.

Execution timeout works by wrapping each job in `context.WithTimeout`. Jobs should respect `ctx` and pass it to downstream calls (HTTP, DB, etc.). If a job ignores context, the worker returns an error but the goroutine may keep running until the reaper recovers the DB row.

## Package layout

```
gowork/
├── client.go, job.go, worker.go   # Public API
├── migrate.go, schema.go          # Setup and validation
├── cmd/worker/main.go             # Example worker binary
├── examples/ping/ping.go          # Example job handler
└── internal/
    ├── store/                     # sqlc-generated Postgres access
    ├── registry/                  # Slug → handler registry
    └── worker/                    # Claim loop, pool, reaper
```

| Package | Role |
|---------|------|
| `gowork` (root) | Public API — `Migrate`, `Open`, `Register`, `Enqueue`, worker constructors |
| `internal/store` | Postgres queries (sqlc-generated, not exported) |
| `internal/registry` | Handler registry used by workers |
| `internal/worker` | Worker implementation |
| `examples/ping` | Sample job handler |
| `cmd/worker` | Runnable worker binary |

Job handlers live in **your application**, not in this library. Each handler is a struct that implements `gowork.Job` and registers itself in `init()`. The worker binary blank-imports handler packages to trigger registration:

```go
import _ "github.com/thalestmm/gowork/examples/ping"
```

## Implementing a new job

Every job is a struct in your app that implements the `gowork.Job` interface and registers itself in `init()`.

### 1. Create a new file

```go
package jobs

import (
    "context"
    "encoding/json"

    gowork "github.com/thalestmm/gowork"
)

type SendEmailJob struct {
    To      string `json:"to"`
    Subject string `json:"subject"`
    Body    string `json:"body"`
}
```

The JSON tags define the payload shape stored in the `payload` JSONB column.

### 2. Implement the interface

```go
func (j *SendEmailJob) Slug() string { return "send_email" }

func (j *SendEmailJob) Params() json.RawMessage {
    raw, _ := json.Marshal(j)
    return raw
}

func (j *SendEmailJob) ParseParams(raw json.RawMessage) error {
    return json.Unmarshal(raw, j)
}

func (j *SendEmailJob) Execute(ctx context.Context) error {
    return sendEmail(ctx, j.To, j.Subject, j.Body)
}
```

### 3. Register the slug

```go
func init() {
    gowork.Register("send_email", func() gowork.Job { return &SendEmailJob{} })
}
```

The `slug` string must match the `slug` field when enqueueing. If a worker claims a row whose slug has no registered handler, the job is immediately failed.

See [`examples/ping/ping.go`](examples/ping/ping.go) for a working example.

### Guidelines

- **One file per job type** — keeps the registry easy to navigate as it grows.
- **Keep `Execute` idempotent when possible** — jobs may retry after failure.
- **Respect `ctx`** — check `ctx.Err()` and pass `ctx` to I/O so timeouts and shutdown work.
- **Return descriptive errors** — they are stored in the `errors` array on the job row.
- **No changes to workers required** — registering a new slug is enough for `SimpleWorker` to pick it up.

## Enqueueing jobs

Use `client.Enqueue`:

```go
payload, _ := json.Marshal(map[string]string{
    "to":      "user@example.com",
    "subject": "Hello",
    "body":    "Welcome!",
})

maxAttempts := int32(3)
id, err := client.Enqueue(ctx, gowork.EnqueueOpts{
    Slug:        "send_email",
    Payload:     payload,
    Priority:    0,
    MaxAttempts: &maxAttempts, // nil = unlimited retries
})
```

### Job row fields

| Column | Type | Description |
|--------|------|-------------|
| `id` | UUID | Primary key (auto-generated if omitted) |
| `slug` | text | Maps to a registered handler |
| `payload` | JSONB | Arguments for the handler |
| `priority` | int | Higher values are picked first |
| `max_attempts` | int, nullable | `NULL` = unlimited retries |
| `status` | text | `pending`, `running`, `completed`, `failed` |
| `attempts` | int | Incremented on each claim |
| `logs` | text[] | Append-only execution log |
| `errors` | text[] | Append-only error messages |
| `created_at` | timestamptz | Set on insert |
| `started_at` | timestamptz | Set on claim |
| `ended_at` | timestamptz | Set on completion or terminal failure |

## Configuration

All settings are read from environment variables by the example worker binary:

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgres://postgres:postgres@localhost:5432/gowork?sslmode=disable` | Postgres connection string |
| `WORKER_CONCURRENCY` | `4` | Goroutines per worker type |
| `WORKER_POLL_INTERVAL` | `1s` | Sleep when no jobs are available |
| `JOB_TIMEOUT` | `5m` | Max execution time per job |
| `STALE_JOB_TIMEOUT` | `10m` | Reclaim jobs stuck in `running` |
| `REAPER_INTERVAL` | `1m` | Stale-job reaper tick interval |

Duration values use Go's duration format (`30s`, `5m`, `1h`).

## Running multiple worker types

`gowork.RunConcurrent` accepts multiple workers and spawns `concurrency` goroutines for each:

```go
cfg := gowork.WorkerConfig{
    Poll:       gowork.DefaultPollInterval,
    JobTimeout: gowork.DefaultJobTimeout,
}

gowork.RunConcurrent(ctx, 4,
    client.NewSimpleWorker(cfg),
    client.NewSpecificTaskWorker(cfg, "send_email"),
)
```

Each goroutine runs an independent claim loop. Because claiming uses `SKIP LOCKED`, they will not interfere with each other.

## Testing

Tests are split into two layers:

| Layer | Command | Requires Docker |
|-------|---------|-----------------|
| Unit | `just test` or `go test ./...` | No |
| Integration | `just test-integration` or `go test -tags=integration ./...` | Yes |

Integration tests use [testcontainers](https://golang.testcontainers.org/) to spin up Postgres 16 and exercise real SQL (`SKIP LOCKED`, claim ordering, fail/retry, stale reaper, worker lifecycle, schema validation).

### What's covered

- **Unit** — job registry/parsing, config defaults, `RunConcurrent`, execution timeouts, `SchemaError` formatting
- **Integration** — complete/fail/retry flows, max attempts, unknown slug, job timeout, claim ordering, priority/specific workers, concurrent claims, stale-job reaper, schema mismatch detection

Test helpers live in [`internal/testutil/`](internal/testutil/) (`StartPostgres`, `InsertJob`, test job handlers). Integration test files use the `//go:build integration` tag.

## Development

### Prerequisites

- Go 1.26+
- Postgres (or Docker for integration tests)
- [sqlc](https://sqlc.dev/) for code generation

### Setup

```bash
# Regenerate store code after changing SQL
just generate

# Build and run the worker
just build
./bin/worker

# Or run directly
just run
```

### Changing SQL queries

1. Edit files in `internal/store/queries/`.
2. Run `just generate` (or `sqlc generate`).
3. Update Go code if query signatures changed.

Type overrides for sqlc are in [`sqlc.yaml`](sqlc.yaml). All columns map to stdlib Go types (`int32`, `*int32`, `string`, `[]string`, `json.RawMessage`, `time.Time`, `uuid.UUID`).

### Enqueue a test job

```sql
INSERT INTO jobs (id, slug, payload, priority)
VALUES (
    gen_random_uuid(),
    'ping',
    '{"message": "hello"}',
    0
);
```

Then start the worker and watch the logs.

## Production notes

- **Indexes** — add an index on `(status, priority DESC, created_at ASC)` for faster claims; a partial index on `WHERE status = 'pending'` helps further.
- **Separate processes** — run the API (enqueue) and workers as separate binaries or containers; scale workers horizontally.
- **Observability** — consider structured logging, metrics for claim/complete/fail rates, and alerting on growing `pending` or `failed` counts.
- **LISTEN/NOTIFY** — replace polling with a Postgres notification on insert to wake workers instantly.
- **Scheduled jobs** — add a `run_at` column and filter `run_at <= NOW()` in claim queries.
