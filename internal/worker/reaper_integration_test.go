//go:build integration

package worker

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/thalestmm/gowork/internal/testutil"
	"github.com/thalestmm/gowork/jobs"
	"github.com/thalestmm/gowork/repository"
)

func TestReapStaleJobs_resetsToPending(t *testing.T) {
	db := testutil.StartPostgres(t)
	testutil.RegisterTestJobs(t)
	testutil.TruncateJobs(t, db)

	job := testutil.InsertJob(t, db, testutil.InsertOpts{Slug: testutil.SlugSuccess})
	claimed, err := db.Queries.ClaimNextPendingJob(context.Background())
	require.NoError(t, err)
	require.Equal(t, jobs.StatusRunning, claimed.Status)

	testutil.SetJobStartedAt(t, db, job.ID, time.Now().Add(-time.Hour))

	cutoff := time.Now().Add(-30 * time.Minute)
	n, err := db.Queries.ReapStaleJobs(context.Background(), repository.ReapStaleJobsParams{
		ErrorMessage: "stale test",
		Cutoff:       &cutoff,
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	got := testutil.GetJob(t, db, job.ID)
	require.Equal(t, jobs.StatusPending, got.Status)
	require.Nil(t, got.StartedAt)
	require.Contains(t, got.Errors[0], "stale test")
}

func TestReapStaleJobs_failsWhenMaxAttempts(t *testing.T) {
	db := testutil.StartPostgres(t)
	testutil.RegisterTestJobs(t)
	testutil.TruncateJobs(t, db)

	maxAttempts := int32(1)
	job := testutil.InsertJob(t, db, testutil.InsertOpts{
		Slug:        testutil.SlugSuccess,
		MaxAttempts: &maxAttempts,
	})

	claimed, err := db.Queries.ClaimNextPendingJob(context.Background())
	require.NoError(t, err)
	require.Equal(t, int32(1), claimed.Attempts)

	testutil.SetJobStartedAt(t, db, job.ID, time.Now().Add(-time.Hour))

	cutoff := time.Now().Add(-30 * time.Minute)
	_, err = db.Queries.ReapStaleJobs(context.Background(), repository.ReapStaleJobsParams{
		ErrorMessage: "stale test",
		Cutoff:       &cutoff,
	})
	require.NoError(t, err)

	got := testutil.GetJob(t, db, job.ID)
	require.Equal(t, jobs.StatusFailed, got.Status)
	require.NotNil(t, got.EndedAt)
}

func TestReapStaleJobs_ignoresFreshRunning(t *testing.T) {
	db := testutil.StartPostgres(t)
	testutil.RegisterTestJobs(t)
	testutil.TruncateJobs(t, db)

	job := testutil.InsertJob(t, db, testutil.InsertOpts{Slug: testutil.SlugSuccess})
	_, err := db.Queries.ClaimNextPendingJob(context.Background())
	require.NoError(t, err)

	cutoff := time.Now().Add(-30 * time.Minute)
	n, err := db.Queries.ReapStaleJobs(context.Background(), repository.ReapStaleJobsParams{
		ErrorMessage: "stale test",
		Cutoff:       &cutoff,
	})
	require.NoError(t, err)
	require.Equal(t, int64(0), n)

	got := testutil.GetJob(t, db, job.ID)
	require.Equal(t, jobs.StatusRunning, got.Status)
}

func TestReapStaleJobs_ignoresPendingAndCompleted(t *testing.T) {
	db := testutil.StartPostgres(t)
	testutil.RegisterTestJobs(t)
	testutil.TruncateJobs(t, db)

	pending := testutil.InsertJob(t, db, testutil.InsertOpts{Slug: testutil.SlugSuccess})
	completed := testutil.InsertJob(t, db, testutil.InsertOpts{Slug: testutil.SlugSuccess})
	err := db.Queries.CompleteJob(context.Background(), completed.ID)
	require.NoError(t, err)

	cutoff := time.Now().Add(-30 * time.Minute)
	n, err := db.Queries.ReapStaleJobs(context.Background(), repository.ReapStaleJobsParams{
		ErrorMessage: "stale test",
		Cutoff:       &cutoff,
	})
	require.NoError(t, err)
	require.Equal(t, int64(0), n)

	require.Equal(t, jobs.StatusPending, testutil.GetJob(t, db, pending.ID).Status)
	require.Equal(t, jobs.StatusCompleted, testutil.GetJob(t, db, completed.ID).Status)
}

func TestRunStaleJobReaper_loop(t *testing.T) {
	db := testutil.StartPostgres(t)
	testutil.RegisterTestJobs(t)
	testutil.TruncateJobs(t, db)

	job := testutil.InsertJob(t, db, testutil.InsertOpts{Slug: testutil.SlugSuccess})
	_, err := db.Queries.ClaimNextPendingJob(context.Background())
	require.NoError(t, err)
	testutil.SetJobStartedAt(t, db, job.ID, time.Now().Add(-time.Hour))

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	go func() {
		_ = RunStaleJobReaper(ctx, db.Queries, 50*time.Millisecond, 30*time.Minute)
	}()

	require.Eventually(t, func() bool {
		got := testutil.GetJob(t, db, job.ID)
		return got.Status == jobs.StatusPending && len(got.Errors) > 0
	}, 3*time.Second, 50*time.Millisecond)

	got := testutil.GetJob(t, db, job.ID)
	require.Contains(t, fmt.Sprintf("%v", got.Errors), "stale after")
}
