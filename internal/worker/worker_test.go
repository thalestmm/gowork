package worker

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type stubJob struct {
	execute func(ctx context.Context) error
}

func (s stubJob) Slug() string { return "stub" }

func (s stubJob) Params() json.RawMessage { return nil }

func (s stubJob) ParseParams(json.RawMessage) error { return nil }

func (s stubJob) Execute(ctx context.Context) error {
	return s.execute(ctx)
}

func TestRunJob_success(t *testing.T) {
	err := runJob(context.Background(), stubJob{
		execute: func(ctx context.Context) error { return nil },
	})
	require.NoError(t, err)
}

func TestRunJob_executeError(t *testing.T) {
	want := errors.New("execute failed")
	err := runJob(context.Background(), stubJob{
		execute: func(ctx context.Context) error { return want },
	})
	require.Equal(t, want, err)
}

func TestRunJob_timeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := runJob(ctx, stubJob{
		execute: func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
				return nil
			}
		},
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestRunJob_respectsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := runJob(ctx, stubJob{
		execute: func(ctx context.Context) error { return ctx.Err() },
	})
	require.ErrorIs(t, err, context.Canceled)
}
