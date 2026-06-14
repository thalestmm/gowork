package jobs_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/thalestmm/gowork/jobs"
	"github.com/thalestmm/gowork/repository"
)

type echoJob struct {
	Value string `json:"value"`
}

func (j *echoJob) Slug() string { return "echo" }

func (j *echoJob) Params() json.RawMessage {
	raw, _ := json.Marshal(j)
	return raw
}

func (j *echoJob) ParseParams(raw json.RawMessage) error {
	return json.Unmarshal(raw, j)
}

func (j *echoJob) Execute(ctx context.Context) error {
	return ctx.Err()
}

type strictJob struct{}

func (j *strictJob) Slug() string { return "strict" }

func (j *strictJob) Params() json.RawMessage { return nil }

func (j *strictJob) ParseParams(raw json.RawMessage) error {
	if len(raw) == 0 {
		return errors.New("payload required")
	}
	return nil
}

func (j *strictJob) Execute(ctx context.Context) error { return nil }

func TestRegister_NewJob_success(t *testing.T) {
	jobs.ResetRegistry()
	t.Cleanup(jobs.ResetRegistry)
	jobs.Register("echo", func() jobs.Job { return &echoJob{} })

	payload := json.RawMessage(`{"value":"hello"}`)
	row := repository.Job{Slug: "echo", Payload: &payload}

	job, err := jobs.NewJob(row)
	require.NoError(t, err)
	require.Equal(t, "echo", job.Slug())
}

func TestNewJob_unknownSlug(t *testing.T) {
	jobs.ResetRegistry()
	t.Cleanup(jobs.ResetRegistry)

	_, err := jobs.NewJob(repository.Job{Slug: "missing"})
	require.ErrorContains(t, err, `no handler registered for slug "missing"`)
}

func TestNewJob_invalidJSON(t *testing.T) {
	jobs.ResetRegistry()
	t.Cleanup(jobs.ResetRegistry)
	jobs.Register("echo", func() jobs.Job { return &echoJob{} })

	payload := json.RawMessage(`{invalid`)
	row := repository.Job{Slug: "echo", Payload: &payload}

	_, err := jobs.NewJob(row)
	require.ErrorContains(t, err, "parse params")
}

func TestNewJob_nilPayload(t *testing.T) {
	jobs.ResetRegistry()
	t.Cleanup(jobs.ResetRegistry)
	jobs.Register("strict", func() jobs.Job { return &strictJob{} })

	_, err := jobs.NewJob(repository.Job{Slug: "strict", Payload: nil})
	require.ErrorContains(t, err, "payload required")
}

func TestNewQueuedJob_metadata(t *testing.T) {
	jobs.ResetRegistry()
	t.Cleanup(jobs.ResetRegistry)
	jobs.Register("echo", func() jobs.Job { return &echoJob{} })

	id := uuid.New()
	maxAttempts := int32(3)
	payload := json.RawMessage(`{"value":"x"}`)
	row := repository.Job{
		ID:          id,
		Slug:        "echo",
		Payload:     &payload,
		Priority:    7,
		Attempts:    2,
		MaxAttempts: &maxAttempts,
	}

	queued, err := jobs.NewQueuedJob(row)
	require.NoError(t, err)
	require.Equal(t, id, queued.ID)
	require.Equal(t, int32(7), queued.Priority)
	require.Equal(t, int32(2), queued.Attempts)
	require.Equal(t, &maxAttempts, queued.MaxAttempts)
	require.Equal(t, "echo", queued.Job.Slug())
}
