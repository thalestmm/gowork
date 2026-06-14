# gowork

A simple Postgres-backed job queue in Go. Jobs are stored as rows in a `jobs` table, claimed atomically by workers using `FOR UPDATE SKIP LOCKED`, and executed by Go structs registered at startup via a slug-based registry.

## How it works

### Overview

```
Enqueue (InsertJob)          Worker claim loop
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

There are two distinct `Job` types in the codebase:

| Type | Package | Role |
|------|---------|------|
| `repository.Job` | `repository` | sqlc-generated struct mirroring a DB row |
| `jobs.Job` (interface) | `jobs` | Runtime handler with `Execute(ctx)` logic |

Workers bridge the two: they claim a `repository.Job` row, look up a Go handler by `slug`, parse the JSON payload, and call `Execute`.

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

| Worker | Picks up |
|--------|----------|
| `SimpleWorker` | Next pending job (any slug) |
| `PriorityWorker` | Next pending job where `min < priority < max` (exclusive bounds; nil max = no upper bound) |
| `SpecificTaskWorker` | Next pending job matching a single slug |

The worker binary owns orchestration: it configures concurrency, starts goroutines via `worker.RunConcurrent`, and runs the stale-job reaper in the background. Each worker's `Run(ctx)` is a blocking loop, which keeps the logic easy to test.

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

Job handlers live in the `jobs/` package. Each job type gets its own file; registration happens in `init()` so handlers are available as soon as the package is imported.

```
jobs/
├── job.go          # Job interface, slug registry, NewQueuedJob
├── ping.go         # Example: PingJob
└── send_email.go   # Your new job (one file per slug)
```

| File | Responsibility |
|------|----------------|
| `jobs/job.go` | Shared types — `Job` interface, `Register`, `NewJob`, `QueuedJob` |
| `jobs/<name>.go` | One concrete job type + `init()` registration |

The worker binary imports the package to trigger registration:

```go
import _ "github.com/thalestmm/gowork/jobs"
```

If you split jobs across multiple packages later (e.g. `jobs/email/`, `jobs/billing/`), each subpackage registers its handlers the same way and the worker imports them all.

Worker infrastructure lives in `internal/worker/`:

```
internal/worker/
├── config.go       # Config, timeout defaults
├── worker.go       # SimpleWorker, PriorityWorker, SpecificTaskWorker
├── pool.go         # RunConcurrent — spawns goroutine pools
└── reaper.go       # RunStaleJobReaper — recovers stuck jobs
```

`cmd/worker/main.go` is the only binary for now. It wires env config, the DB pool, the reaper, and `worker.RunConcurrent`. A future `cmd/api/main.go` would enqueue jobs via `repository.InsertJob` without importing `internal/worker`.

## Implementing a new job

Every job is a struct in the `jobs` package that implements the `Job` interface and registers itself in `init()`.

### 1. Create a new file

Add `jobs/send_email.go`:

```go
package jobs

import (
    "context"
    "encoding/json"
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
    // Always respect ctx — pass it to HTTP, DB, and other blocking calls.
    return sendEmail(ctx, j.To, j.Subject, j.Body)
}
```

### 3. Register the slug

```go
func init() {
    Register("send_email", func() Job { return &SendEmailJob{} })
}
```

The `slug` string must match the `slug` column when enqueueing. If a worker claims a row whose slug has no registered handler, the job is immediately failed.

See [`jobs/ping.go`](jobs/ping.go) for a working example.

### Guidelines

- **One file per job type** — keeps the registry easy to navigate as it grows.
- **Keep `Execute` idempotent when possible** — jobs may retry after failure.
- **Respect `ctx`** — check `ctx.Err()` and pass `ctx` to I/O so timeouts and shutdown work.
- **Return descriptive errors** — they are stored in the `errors` array on the job row.
- **No changes to workers required** — registering a new slug is enough for `SimpleWorker` to pick it up.

## Enqueueing jobs

Use the generated `InsertJob` query from the `repository` package:

```go
import (
    "encoding/json"
    "github.com/google/uuid"
    "github.com/thalestmm/gowork/repository"
)

payload, _ := json.Marshal(map[string]string{
    "to":      "user@example.com",
    "subject": "Hello",
    "body":    "Welcome!",
})

maxAttempts := int32(3)
job, err := queries.InsertJob(ctx, repository.InsertJobParams{
    ID:          uuid.New(),
    Slug:        "send_email",
    Payload:     (*json.RawMessage)(&payload),
    Priority:    0,
    MaxAttempts: &maxAttempts, // nil = unlimited retries
})
```

### Job row fields

| Column | Type | Description |
|--------|------|-------------|
| `id` | UUID | Primary key (generate client-side) |
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

All settings are read from environment variables:

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

`worker.RunConcurrent` accepts multiple workers and spawns `concurrency` goroutines for each:

```go
import "github.com/thalestmm/gowork/internal/worker"

cfg := worker.Config{Poll: time.Second, JobTimeout: 5 * time.Minute}

worker.RunConcurrent(ctx, 4,
    worker.NewSimple(queries, cfg),
    worker.NewSpecificTask(queries, cfg, "send_email"),
)
```

Each goroutine runs an independent claim loop. Because claiming uses `SKIP LOCKED`, they will not interfere with each other.

## Project structure

```
gowork/
├── cmd/
│   └── worker/
│       └── main.go                  # Worker binary entrypoint
├── internal/
│   └── worker/
│       ├── config.go                # Worker config and defaults
│       ├── worker.go                # Worker types and claim loop
│       ├── pool.go                  # RunConcurrent
│       └── reaper.go                # Stale-job reaper
├── jobs/
│   ├── job.go                       # Job interface, registry, QueuedJob
│   └── ping.go                      # Example job handler
├── repository/
│   ├── migrations/                  # Goose SQL migrations
│   │   └── 001_initial_schema.sql
│   ├── queries/                     # sqlc query definitions
│   │   └── jobs.sql
│   ├── models.go                    # Generated row types
│   └── jobs.sql.go                  # Generated query methods
├── sqlc.yaml                        # sqlc config (stdlib Go types)
└── justfile                         # Task runner (sqlc generate, etc.)
```

| Package | Role |
|---------|------|
| `cmd/worker` | Binary — env config, DB pool, start workers |
| `internal/worker` | Claim loop, concurrency pool, stale-job reaper |
| `jobs` | Job handlers registered by slug |
| `repository` | Postgres access (sqlc-generated) |

## Testing

Tests are split into two layers:

| Layer | Command | Requires Docker |
|-------|---------|-----------------|
| Unit | `just test` or `go test ./...` | No |
| Integration | `just test-integration` or `go test -tags=integration ./...` | Yes |

Integration tests use [testcontainers](https://golang.testcontainers.org/) to spin up Postgres 16 and exercise real SQL (`SKIP LOCKED`, claim ordering, fail/retry, stale reaper, worker lifecycle).

### What's covered

- **Unit** — job registry/parsing, config defaults, `RunConcurrent`, execution timeouts via `runJob`
- **Integration** — complete/fail/retry flows, max attempts, unknown slug, job timeout, claim ordering, priority/specific workers, concurrent claims, stale-job reaper

Test helpers live in [`internal/testutil/`](internal/testutil/) (`StartPostgres`, `InsertJob`, test job handlers). Integration test files use the `//go:build integration` tag.

## Development

### Prerequisites

- Go 1.26+
- Postgres
- [sqlc](https://sqlc.dev/) for code generation
- [goose](https://github.com/pressly/goose) for migrations (optional)

### Setup

```bash
# Run migrations against your database
goose -dir repository/migrations postgres "$DATABASE_URL" up

# Regenerate repository code after changing SQL
just generate

# Build and run the worker
go build -o bin/worker ./cmd/worker
./bin/worker

# Or run directly
go run ./cmd/worker
```

### Changing SQL queries

1. Edit files in `repository/queries/`.
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
