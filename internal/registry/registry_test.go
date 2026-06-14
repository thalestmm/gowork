package registry_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/thalestmm/gowork/internal/registry"
	"github.com/thalestmm/gowork/internal/store"
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
	registry.ResetRegistry()
	t.Cleanup(registry.ResetRegistry)
	registry.Register("echo", func() registry.Job { return &echoJob{} })

	payload := json.RawMessage(`{"value":"hello"}`)
	row := store.Job{Slug: "echo", Payload: &payload}

	job, err := registry.NewJob(row)
	require.NoError(t, err)
	require.Equal(t, "echo", job.Slug())
}

func TestNewJob_unknownSlug(t *testing.T) {
	registry.ResetRegistry()
	t.Cleanup(registry.ResetRegistry)

	_, err := registry.NewJob(store.Job{Slug: "missing"})
	require.ErrorContains(t, err, `no handler registered for slug "missing"`)
}

func TestNewJob_invalidJSON(t *testing.T) {
	registry.ResetRegistry()
	t.Cleanup(registry.ResetRegistry)
	registry.Register("echo", func() registry.Job { return &echoJob{} })

	payload := json.RawMessage(`{invalid`)
	row := store.Job{Slug: "echo", Payload: &payload}

	_, err := registry.NewJob(row)
	require.ErrorContains(t, err, "parse params")
}

func TestNewJob_nilPayload(t *testing.T) {
	registry.ResetRegistry()
	t.Cleanup(registry.ResetRegistry)
	registry.Register("strict", func() registry.Job { return &strictJob{} })

	_, err := registry.NewJob(store.Job{Slug: "strict", Payload: nil})
	require.ErrorContains(t, err, "payload required")
}

func TestNewQueuedJob_metadata(t *testing.T) {
	registry.ResetRegistry()
	t.Cleanup(registry.ResetRegistry)
	registry.Register("echo", func() registry.Job { return &echoJob{} })

	id := uuid.New()
	maxAttempts := int32(3)
	payload := json.RawMessage(`{"value":"x"}`)
	row := store.Job{
		ID:          id,
		Slug:        "echo",
		Payload:     &payload,
		Priority:    7,
		Attempts:    2,
		MaxAttempts: &maxAttempts,
	}

	queued, err := registry.NewQueuedJob(row)
	require.NoError(t, err)
	require.Equal(t, id, queued.ID)
	require.Equal(t, int32(7), queued.Priority)
	require.Equal(t, int32(2), queued.Attempts)
	require.Equal(t, &maxAttempts, queued.MaxAttempts)
	require.Equal(t, "echo", queued.Job.Slug())
}
