package worker

import (
	"log/slog"
	"time"
)

const (
	DefaultPollInterval   = time.Second
	DefaultJobTimeout     = 5 * time.Minute
	DefaultStaleJobAfter  = 10 * time.Minute
	DefaultReaperInterval = time.Minute
)

type Config struct {
	Poll       time.Duration
	JobTimeout time.Duration
	Logger     *slog.Logger
}

func (c Config) withDefaults() Config {
	if c.Poll <= 0 {
		c.Poll = DefaultPollInterval
	}
	if c.JobTimeout <= 0 {
		c.JobTimeout = DefaultJobTimeout
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	return c
}
