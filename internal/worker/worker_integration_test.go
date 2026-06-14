//go:build integration

package worker

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/thalestmm/gowork/internal/testutil"
	"github.com/thalestmm/gowork/jobs"
	"github.com/thalestmm/gowork/repository"
)

func fastConfig(timeout time.Duration) Config {
	return Config{
		Poll:       10 * time.Millisecond,
		JobTimeout: timeout,
	}
}

func startSimpleWorker(t *testing.T, db *testutil.DB, cfg Config) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	w := NewSimple(db.Queries, cfg)
	go func() {
		_ = w.Run(ctx)
	}()
	return cancel
}

func waitForJobStatus(t *testing.T, db *testutil.DB, id uuid.UUID, status string) {
	t.Helper()
	require.Eventually(t, func() bool {
		job := testutil.GetJob(t, db, id)
		return job.Status == status
	}, 5*time.Second, 20*time.Millisecond)
}

func TestSimpleWorker_completesJob(t *testing.T) {
	db := testutil.StartPostgres(t)
	testutil.RegisterTestJobs(t)
	testutil.TruncateJobs(t, db)

	job := testutil.InsertJob(t, db, testutil.InsertOpts{Slug: testutil.SlugSuccess})
	cancel := startSimpleWorker(t, db, fastConfig(time.Second))
	defer cancel()

	waitForJobStatus(t, db, job.ID, jobs.StatusCompleted)

	got := testutil.GetJob(t, db, job.ID)
	require.NotNil(t, got.EndedAt)
	require.Contains(t, got.Logs, `started "test_success" (attempt 1)`)
	require.Contains(t, got.Logs, "completed")
}

func TestFailJob_retriesToPending(t *testing.T) {
	db := testutil.StartPostgres(t)
	testutil.TruncateJobs(t, db)

	maxAttempts := int32(3)
	job := testutil.InsertJob(t, db, testutil.InsertOpts{
		Slug:        testutil.SlugSuccess,
		MaxAttempts: &maxAttempts,
	})

	claimed, err := db.Queries.ClaimNextPendingJob(context.Background())
	require.NoError(t, err)
	require.Equal(t, int32(1), claimed.Attempts)

	err = db.Queries.FailJob(context.Background(), repository.FailJobParams{
		ErrorMessage: "boom",
		ID:           job.ID,
	})
	require.NoError(t, err)

	got := testutil.GetJob(t, db, job.ID)
	require.Equal(t, jobs.StatusPending, got.Status)
	require.Equal(t, int32(1), got.Attempts)
	require.Contains(t, got.Errors[0], "boom")
	require.Nil(t, got.EndedAt)
}

func TestSimpleWorker_failsPermanently(t *testing.T) {
	db := testutil.StartPostgres(t)
	testutil.RegisterTestJobs(t)
	testutil.TruncateJobs(t, db)

	maxAttempts := int32(1)
	job := testutil.InsertJob(t, db, testutil.InsertOpts{
		Slug:        testutil.SlugFail,
		MaxAttempts: &maxAttempts,
	})
	cancel := startSimpleWorker(t, db, fastConfig(time.Second))
	defer cancel()

	waitForJobStatus(t, db, job.ID, jobs.StatusFailed)

	got := testutil.GetJob(t, db, job.ID)
	require.NotNil(t, got.EndedAt)
}

func TestSimpleWorker_unlimitedRetries(t *testing.T) {
	db := testutil.StartPostgres(t)
	testutil.RegisterTestJobs(t)
	testutil.TruncateJobs(t, db)

	job := testutil.InsertJob(t, db, testutil.InsertOpts{Slug: testutil.SlugFail})
	cancel := startSimpleWorker(t, db, fastConfig(time.Second))
	defer cancel()

	require.Eventually(t, func() bool {
		got := testutil.GetJob(t, db, job.ID)
		return got.Status == jobs.StatusPending && got.Attempts >= 2
	}, 10*time.Second, 50*time.Millisecond)
}

func TestSimpleWorker_unknownSlug(t *testing.T) {
	db := testutil.StartPostgres(t)
	testutil.RegisterTestJobs(t)
	testutil.TruncateJobs(t, db)

	job := testutil.InsertJob(t, db, testutil.InsertOpts{Slug: "no_handler"})
	cancel := startSimpleWorker(t, db, fastConfig(time.Second))
	defer cancel()

	require.Eventually(t, func() bool {
		got := testutil.GetJob(t, db, job.ID)
		return got.Attempts >= 1 && len(got.Errors) > 0
	}, 5*time.Second, 20*time.Millisecond)

	got := testutil.GetJob(t, db, job.ID)
	require.Contains(t, got.Errors[0], "no handler registered")
}

func TestSimpleWorker_jobTimeout(t *testing.T) {
	db := testutil.StartPostgres(t)
	testutil.RegisterTestJobs(t)
	testutil.TruncateJobs(t, db)

	job := testutil.InsertJob(t, db, testutil.InsertOpts{Slug: testutil.SlugSlow})
	cancel := startSimpleWorker(t, db, fastConfig(50*time.Millisecond))
	defer cancel()

	require.Eventually(t, func() bool {
		got := testutil.GetJob(t, db, job.ID)
		return got.Status == jobs.StatusPending && len(got.Errors) > 0
	}, 5*time.Second, 50*time.Millisecond)

	got := testutil.GetJob(t, db, job.ID)
	require.Contains(t, got.Errors[0], "deadline exceeded")
}

func TestSimpleWorker_priorityOrdering(t *testing.T) {
	db := testutil.StartPostgres(t)
	testutil.RegisterTestJobs(t)
	testutil.TruncateJobs(t, db)

	_ = testutil.InsertJob(t, db, testutil.InsertOpts{Slug: testutil.SlugSuccess, Priority: 1})
	high := testutil.InsertJob(t, db, testutil.InsertOpts{Slug: testutil.SlugSuccess, Priority: 10})

	claimed, err := db.Queries.ClaimNextPendingJob(context.Background())
	require.NoError(t, err)
	require.Equal(t, high.ID, claimed.ID)
}

func TestSimpleWorker_fifoWithinPriority(t *testing.T) {
	db := testutil.StartPostgres(t)
	testutil.RegisterTestJobs(t)
	testutil.TruncateJobs(t, db)

	older := testutil.InsertJob(t, db, testutil.InsertOpts{Slug: testutil.SlugSuccess, Priority: 5})
	newer := testutil.InsertJob(t, db, testutil.InsertOpts{Slug: testutil.SlugSuccess, Priority: 5})
	testutil.SetJobCreatedAt(t, db, older.ID, time.Now().Add(-time.Hour))
	testutil.SetJobCreatedAt(t, db, newer.ID, time.Now())

	claimed, err := db.Queries.ClaimNextPendingJob(context.Background())
	require.NoError(t, err)
	require.Equal(t, older.ID, claimed.ID)
}

func TestSimpleWorker_appendsLogs(t *testing.T) {
	db := testutil.StartPostgres(t)
	testutil.RegisterTestJobs(t)
	testutil.TruncateJobs(t, db)

	job := testutil.InsertJob(t, db, testutil.InsertOpts{Slug: testutil.SlugSuccess})
	cancel := startSimpleWorker(t, db, fastConfig(time.Second))
	defer cancel()

	waitForJobStatus(t, db, job.ID, jobs.StatusCompleted)

	got := testutil.GetJob(t, db, job.ID)
	require.GreaterOrEqual(t, len(got.Logs), 2)
}

func TestPriorityWorker_exclusiveBounds(t *testing.T) {
	db := testutil.StartPostgres(t)
	testutil.RegisterTestJobs(t)
	testutil.TruncateJobs(t, db)

	low := testutil.InsertJob(t, db, testutil.InsertOpts{Slug: testutil.SlugSuccess, Priority: 5})
	mid := testutil.InsertJob(t, db, testutil.InsertOpts{Slug: testutil.SlugSuccess, Priority: 10})
	high := testutil.InsertJob(t, db, testutil.InsertOpts{Slug: testutil.SlugSuccess, Priority: 15})

	maxPriority := 15
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := NewPriority(db.Queries, fastConfig(time.Second), 5, &maxPriority)
	go func() { _ = w.Run(ctx) }()

	waitForJobStatus(t, db, mid.ID, jobs.StatusCompleted)

	require.Equal(t, jobs.StatusPending, testutil.GetJob(t, db, low.ID).Status)
	require.Equal(t, jobs.StatusPending, testutil.GetJob(t, db, high.ID).Status)
}

func TestPriorityWorker_noUpperBound(t *testing.T) {
	db := testutil.StartPostgres(t)
	testutil.RegisterTestJobs(t)
	testutil.TruncateJobs(t, db)

	low := testutil.InsertJob(t, db, testutil.InsertOpts{Slug: testutil.SlugSuccess, Priority: 5})
	high := testutil.InsertJob(t, db, testutil.InsertOpts{Slug: testutil.SlugSuccess, Priority: 20})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := NewPriority(db.Queries, fastConfig(time.Second), 5, nil)
	go func() { _ = w.Run(ctx) }()

	waitForJobStatus(t, db, high.ID, jobs.StatusCompleted)
	require.Equal(t, jobs.StatusPending, testutil.GetJob(t, db, low.ID).Status)
}

func TestSpecificTaskWorker_filtersSlug(t *testing.T) {
	db := testutil.StartPostgres(t)
	testutil.RegisterTestJobs(t)
	testutil.TruncateJobs(t, db)

	other := testutil.InsertJob(t, db, testutil.InsertOpts{Slug: "send_email"})
	target := testutil.InsertJob(t, db, testutil.InsertOpts{Slug: testutil.SlugSuccess})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := NewSpecificTask(db.Queries, fastConfig(time.Second), testutil.SlugSuccess)
	go func() { _ = w.Run(ctx) }()

	waitForJobStatus(t, db, target.ID, jobs.StatusCompleted)
	require.Equal(t, jobs.StatusPending, testutil.GetJob(t, db, other.ID).Status)
}

func TestSimpleWorker_concurrentClaims(t *testing.T) {
	db := testutil.StartPostgres(t)
	testutil.RegisterTestJobs(t)
	testutil.TruncateJobs(t, db)

	ids := make([]uuid.UUID, 10)
	for i := range ids {
		job := testutil.InsertJob(t, db, testutil.InsertOpts{Slug: testutil.SlugSuccess})
		ids[i] = job.ID
	}

	w := NewSimple(db.Queries, fastConfig(time.Second))
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		_ = RunConcurrent(ctx, 5, w)
	}()

	require.Eventually(t, func() bool {
		for _, id := range ids {
			if testutil.GetJob(t, db, id).Status != jobs.StatusCompleted {
				return false
			}
		}
		return true
	}, 10*time.Second, 50*time.Millisecond)

	cancel()
}
