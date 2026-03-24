package domain

import "time"

// LogEntry represents a single log line received from a client application.
type LogEntry struct {
	// Timestamp is when the log event occurred (client-provided or server-assigned).
	Timestamp time.Time         `json:"timestamp"`
	Service   string            `json:"service"`
	Level     string            `json:"level"`
	Message   string            `json:"message"`
	Labels    map[string]string `json:"labels,omitempty"`
}
