package logging

import (
	"log/slog"
	"os"
	"strings"
)

// Setup configures the default slog logger from LOG_LEVEL and LOG_FORMAT env
// vars and returns a logger tagged with the given component name.
func Setup(component string) *slog.Logger {
	level := parseLevel(os.Getenv("LOG_LEVEL"))

	opts := &slog.HandlerOptions{Level: level}
	var handler slog.Handler
	switch strings.ToLower(os.Getenv("LOG_FORMAT")) {
	case "json":
		handler = slog.NewJSONHandler(os.Stdout, opts)
	default:
		handler = slog.NewTextHandler(os.Stdout, opts)
	}

	logger := slog.New(handler).With("component", component)
	slog.SetDefault(logger)
	return logger
}

func parseLevel(raw string) slog.Level {
	switch strings.ToLower(raw) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
