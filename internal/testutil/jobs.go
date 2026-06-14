package testutil

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/thalestmm/gowork/jobs"
)

const (
	SlugSuccess = "test_success"
	SlugFail    = "test_fail"
	SlugSlow    = "test_slow"
)

func RegisterTestJobs(t *testing.T) {
	t.Helper()
	jobs.ResetRegistry()
	RegisterTestJobsWithoutCleanup()
	t.Cleanup(jobs.ResetRegistry)
}

func RegisterTestJobsWithoutCleanup() {
	jobs.Register(SlugSuccess, func() jobs.Job { return &SuccessJob{} })
	jobs.Register(SlugFail, func() jobs.Job { return &FailJob{Err: errors.New("test failure")} })
	jobs.Register(SlugSlow, func() jobs.Job { return &SlowJob{Delay: 10 * time.Second} })
}

type SuccessJob struct{}

func (j *SuccessJob) Slug() string { return SlugSuccess }

func (j *SuccessJob) Params() json.RawMessage { return json.RawMessage(`{}`) }

func (j *SuccessJob) ParseParams(json.RawMessage) error { return nil }

func (j *SuccessJob) Execute(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

type FailJob struct {
	Err error
}

func (j *FailJob) Slug() string { return SlugFail }

func (j *FailJob) Params() json.RawMessage { return json.RawMessage(`{}`) }

func (j *FailJob) ParseParams(json.RawMessage) error { return nil }

func (j *FailJob) Execute(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return j.Err
}

type SlowJob struct {
	Delay time.Duration
}

func (j *SlowJob) Slug() string { return SlugSlow }

func (j *SlowJob) Params() json.RawMessage { return json.RawMessage(`{}`) }

func (j *SlowJob) ParseParams(json.RawMessage) error { return nil }

func (j *SlowJob) Execute(ctx context.Context) error {
	timer := time.NewTimer(j.Delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
