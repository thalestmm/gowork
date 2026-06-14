package worker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConfig_withDefaults(t *testing.T) {
	cfg := Config{}.withDefaults()
	require.Equal(t, DefaultPollInterval, cfg.Poll)
	require.Equal(t, DefaultJobTimeout, cfg.JobTimeout)

	custom := Config{
		Poll:       250 * time.Millisecond,
		JobTimeout: 30 * time.Second,
	}.withDefaults()
	require.Equal(t, 250*time.Millisecond, custom.Poll)
	require.Equal(t, 30*time.Second, custom.JobTimeout)
}
