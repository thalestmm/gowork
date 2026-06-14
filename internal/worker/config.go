package worker

import "time"

const (
	DefaultPollInterval   = time.Second
	DefaultJobTimeout     = 5 * time.Minute
	DefaultStaleJobAfter  = 10 * time.Minute
	DefaultReaperInterval = time.Minute
)

type Config struct {
	Poll       time.Duration
	JobTimeout time.Duration
}

func (c Config) withDefaults() Config {
	if c.Poll <= 0 {
		c.Poll = DefaultPollInterval
	}
	if c.JobTimeout <= 0 {
		c.JobTimeout = DefaultJobTimeout
	}
	return c
}
