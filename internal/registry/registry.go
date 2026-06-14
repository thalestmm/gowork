package registry

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/thalestmm/gowork/internal/store"
)

type Job interface {
	Slug() string
	Params() json.RawMessage
	ParseParams(json.RawMessage) error
	Execute(ctx context.Context) error
}

type Factory func() Job

var registry = map[string]Factory{}

func Register(slug string, factory Factory) {
	registry[slug] = factory
}

// ResetRegistry clears all registered handlers. For tests only.
func ResetRegistry() {
	registry = map[string]Factory{}
}

func NewJob(row store.Job) (Job, error) {
	factory, ok := registry[row.Slug]
	if !ok {
		return nil, fmt.Errorf("no handler registered for slug %q", row.Slug)
	}

	job := factory()
	var payload json.RawMessage
	if row.Payload != nil {
		payload = *row.Payload
	}
	if err := job.ParseParams(payload); err != nil {
		return nil, fmt.Errorf("parse params for slug %q: %w", row.Slug, err)
	}
	return job, nil
}

type QueuedJob struct {
	ID          uuid.UUID
	Slug        string
	Priority    int32
	Attempts    int32
	MaxAttempts *int32
	Job         Job
}

func NewQueuedJob(row store.Job) (QueuedJob, error) {
	job, err := NewJob(row)
	if err != nil {
		return QueuedJob{}, err
	}
	return QueuedJob{
		ID:          row.ID,
		Slug:        row.Slug,
		Priority:    row.Priority,
		Attempts:    row.Attempts,
		MaxAttempts: row.MaxAttempts,
		Job:         job,
	}, nil
}
