-- +goose up

CREATE TABLE IF NOT EXISTS jobs (
  id UUID PRIMARY KEY,
  slug TEXT NOT NULL,
  payload JSONB,
  priority INT NOT NULL DEFAULT 0,
  max_attempts INT,
  status TEXT not null DEFAULT 'pending',

  attempts INT NOT NULL DEFAULT 0,
  logs TEXT[],
  errors TEXT[],

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  started_at TIMESTAMPTZ,
  ended_at TIMESTAMPTZ
);

-- +goose down

DROP TABLE IF EXISTS jobs;
