-- name: GetAllJobs :many
SELECT * FROM jobs;

-- name: InsertJob :one
INSERT INTO jobs (id, slug, payload, priority, max_attempts)
VALUES ($1, $2, $3, $4, $5)
RETURNING *;

-- name: ClaimNextPendingJob :one
UPDATE jobs
SET
    status = 'running',
    attempts = attempts + 1,
    started_at = NOW()
WHERE id = (
    SELECT j.id FROM jobs j
    WHERE j.status = 'pending'
    ORDER BY j.priority DESC, j.created_at ASC
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING *;

-- name: ClaimNextPendingJobByPriority :one
UPDATE jobs
SET
    status = 'running',
    attempts = attempts + 1,
    started_at = NOW()
WHERE id = (
    SELECT j.id FROM jobs j
    WHERE j.status = 'pending'
      AND j.priority > sqlc.arg(min_priority)
      AND (sqlc.narg(max_priority)::int IS NULL OR j.priority < sqlc.narg(max_priority))
    ORDER BY j.priority DESC, j.created_at ASC
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING *;

-- name: ClaimNextPendingJobBySlug :one
UPDATE jobs
SET
    status = 'running',
    attempts = attempts + 1,
    started_at = NOW()
WHERE id = (
    SELECT j.id FROM jobs j
    WHERE j.status = 'pending'
      AND j.slug = $1
    ORDER BY j.priority DESC, j.created_at ASC
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING *;

-- name: CompleteJob :exec
UPDATE jobs
SET status = 'completed', ended_at = NOW()
WHERE id = $1;

-- name: FailJob :exec
UPDATE jobs
SET
    status = CASE
        WHEN max_attempts IS NOT NULL AND attempts >= max_attempts THEN 'failed'
        ELSE 'pending'
    END,
    errors = array_append(errors, sqlc.arg(error_message)::text),
    ended_at = CASE
        WHEN max_attempts IS NOT NULL AND attempts >= max_attempts THEN NOW()
        ELSE NULL
    END,
    started_at = CASE
        WHEN max_attempts IS NOT NULL AND attempts >= max_attempts THEN started_at
        ELSE NULL
    END
WHERE id = sqlc.arg(id);

-- name: AppendJobLog :exec
UPDATE jobs
SET logs = array_append(logs, sqlc.arg(log_message)::text)
WHERE id = sqlc.arg(id);

-- name: ReapStaleJobs :execrows
UPDATE jobs
SET
    status = CASE
        WHEN max_attempts IS NOT NULL AND attempts >= max_attempts THEN 'failed'
        ELSE 'pending'
    END,
    errors = array_append(errors, sqlc.arg(error_message)::text),
    ended_at = CASE
        WHEN max_attempts IS NOT NULL AND attempts >= max_attempts THEN NOW()
        ELSE NULL
    END,
    started_at = NULL
WHERE status = 'running'
  AND started_at IS NOT NULL
  AND started_at < sqlc.arg(cutoff);
