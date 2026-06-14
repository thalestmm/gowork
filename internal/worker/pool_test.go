package worker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type fakeWorker struct {
	run func(ctx context.Context) error
}

func (f fakeWorker) Run(ctx context.Context) error {
	return f.run(ctx)
}

func TestRunConcurrent_empty(t *testing.T) {
	err := RunConcurrent(context.Background(), 4)
	require.NoError(t, err)
}

func TestRunConcurrent_zeroConcurrency(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	var runs atomic.Int32
	err := RunConcurrent(ctx, 0, fakeWorker{
		run: func(ctx context.Context) error {
			runs.Add(1)
			<-ctx.Done()
			return ctx.Err()
		},
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, int32(1), runs.Load())
}

func TestRunConcurrent_cancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := RunConcurrent(ctx, 2, fakeWorker{
		run: func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		},
	})
	require.NoError(t, err)
}

func TestRunConcurrent_workerError(t *testing.T) {
	want := errors.New("boom")
	err := RunConcurrent(context.Background(), 2,
		fakeWorker{run: func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		}},
		fakeWorker{run: func(ctx context.Context) error {
			return want
		}},
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "boom")
}

func TestRunConcurrent_processesWork(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{}, 1)
	go func() {
		err := RunConcurrent(ctx, 1, fakeWorker{
			run: func(ctx context.Context) error {
				select {
				case done <- struct{}{}:
				default:
				}
				<-ctx.Done()
				return ctx.Err()
			},
		})
		require.NoError(t, err)
	}()

	require.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)

	cancel()
}
