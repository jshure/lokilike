package logger

import (
	"log/slog"
	"os"
)

// Init configures the global slog default logger. Call once at startup.
// When debug is true, DEBUG-level messages are emitted.
// Output is JSON to stderr for machine-parseable structured logging.
func Init(debug bool) {
	level := slog.LevelInfo
	if debug {
		level = slog.LevelDebug
	}
	handler := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: level,
	})
	slog.SetDefault(slog.New(handler))
}
