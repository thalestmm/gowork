//go:build integration

package worker

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/thalestmm/gowork/internal/store"
	"github.com/thalestmm/gowork/internal/testutil"
)

func TestReapStaleJobs_resetsToPending(t *testing.T) {
	db := testutil.StartPostgres(t)
	testutil.RegisterTestJobs(t)
	testutil.TruncateJobs(t, db)

	job := testutil.InsertJob(t, db, testutil.InsertOpts{Slug: testutil.SlugSuccess})
	claimed, err := db.Store.ClaimNextPendingJob(context.Background())
	require.NoError(t, err)
	require.Equal(t, "running", claimed.Status)

	testutil.SetJobStartedAt(t, db, job.ID, time.Now().Add(-time.Hour))

	cutoff := time.Now().Add(-30 * time.Minute)
	n, err := db.Store.ReapStaleJobs(context.Background(), store.ReapStaleJobsParams{
		ErrorMessage: "stale test",
		Cutoff:       &cutoff,
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	got := testutil.GetJob(t, db, job.ID)
	require.Equal(t, "pending", got.Status)
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

	claimed, err := db.Store.ClaimNextPendingJob(context.Background())
	require.NoError(t, err)
	require.Equal(t, int32(1), claimed.Attempts)

	testutil.SetJobStartedAt(t, db, job.ID, time.Now().Add(-time.Hour))

	cutoff := time.Now().Add(-30 * time.Minute)
	_, err = db.Store.ReapStaleJobs(context.Background(), store.ReapStaleJobsParams{
		ErrorMessage: "stale test",
		Cutoff:       &cutoff,
	})
	require.NoError(t, err)

	got := testutil.GetJob(t, db, job.ID)
	require.Equal(t, "failed", got.Status)
	require.NotNil(t, got.EndedAt)
}

func TestReapStaleJobs_ignoresFreshRunning(t *testing.T) {
	db := testutil.StartPostgres(t)
	testutil.RegisterTestJobs(t)
	testutil.TruncateJobs(t, db)

	job := testutil.InsertJob(t, db, testutil.InsertOpts{Slug: testutil.SlugSuccess})
	_, err := db.Store.ClaimNextPendingJob(context.Background())
	require.NoError(t, err)

	cutoff := time.Now().Add(-30 * time.Minute)
	n, err := db.Store.ReapStaleJobs(context.Background(), store.ReapStaleJobsParams{
		ErrorMessage: "stale test",
		Cutoff:       &cutoff,
	})
	require.NoError(t, err)
	require.Equal(t, int64(0), n)

	got := testutil.GetJob(t, db, job.ID)
	require.Equal(t, "running", got.Status)
}

func TestReapStaleJobs_ignoresPendingAndCompleted(t *testing.T) {
	db := testutil.StartPostgres(t)
	testutil.RegisterTestJobs(t)
	testutil.TruncateJobs(t, db)

	pending := testutil.InsertJob(t, db, testutil.InsertOpts{Slug: testutil.SlugSuccess})
	completed := testutil.InsertJob(t, db, testutil.InsertOpts{Slug: testutil.SlugSuccess})
	err := db.Store.CompleteJob(context.Background(), completed.ID)
	require.NoError(t, err)

	cutoff := time.Now().Add(-30 * time.Minute)
	n, err := db.Store.ReapStaleJobs(context.Background(), store.ReapStaleJobsParams{
		ErrorMessage: "stale test",
		Cutoff:       &cutoff,
	})
	require.NoError(t, err)
	require.Equal(t, int64(0), n)

	require.Equal(t, "pending", testutil.GetJob(t, db, pending.ID).Status)
	require.Equal(t, "completed", testutil.GetJob(t, db, completed.ID).Status)
}

func TestRunStaleJobReaper_loop(t *testing.T) {
	db := testutil.StartPostgres(t)
	testutil.RegisterTestJobs(t)
	testutil.TruncateJobs(t, db)

	job := testutil.InsertJob(t, db, testutil.InsertOpts{Slug: testutil.SlugSuccess})
	_, err := db.Store.ClaimNextPendingJob(context.Background())
	require.NoError(t, err)
	testutil.SetJobStartedAt(t, db, job.ID, time.Now().Add(-time.Hour))

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	go func() {
		_ = RunStaleJobReaper(ctx, db.Store, 50*time.Millisecond, 30*time.Minute)
	}()

	require.Eventually(t, func() bool {
		got := testutil.GetJob(t, db, job.ID)
		return got.Status == "pending" && len(got.Errors) > 0
	}, 3*time.Second, 50*time.Millisecond)

	got := testutil.GetJob(t, db, job.ID)
	require.Contains(t, fmt.Sprintf("%v", got.Errors), "stale after")
}
