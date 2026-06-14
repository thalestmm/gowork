package gowork

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/thalestmm/gowork/internal/store"
)

type Client struct {
	q *store.Queries
}

type Option func(*options)

type options struct {
	skipSchemaCheck bool
}

// WithSkipSchemaCheck disables schema validation. For tests only.
func WithSkipSchemaCheck() Option {
	return func(o *options) {
		o.skipSchemaCheck = true
	}
}

// Open validates the jobs table schema and returns a Client.
// Open never runs migrations — call Migrate separately first.
func Open(ctx context.Context, pool *pgxpool.Pool, opts ...Option) (*Client, error) {
	cfg := options{}
	for _, opt := range opts {
		opt(&cfg)
	}

	if !cfg.skipSchemaCheck {
		if err := ValidateSchema(ctx, pool); err != nil {
			return nil, err
		}
	}

	return &Client{q: store.New(pool)}, nil
}

type EnqueueOpts struct {
	ID          uuid.UUID
	Slug        string
	Payload     json.RawMessage
	Priority    int32
	MaxAttempts *int32
}

// Enqueue inserts a new pending job.
func (c *Client) Enqueue(ctx context.Context, opts EnqueueOpts) (uuid.UUID, error) {
	if opts.ID == uuid.Nil {
		opts.ID = uuid.New()
	}

	var payload *json.RawMessage
	if len(opts.Payload) > 0 {
		p := opts.Payload
		payload = &p
	}

	job, err := c.q.InsertJob(ctx, store.InsertJobParams{
		ID:          opts.ID,
		Slug:        opts.Slug,
		Payload:     payload,
		Priority:    opts.Priority,
		MaxAttempts: opts.MaxAttempts,
	})
	if err != nil {
		return uuid.Nil, err
	}
	return job.ID, nil
}
