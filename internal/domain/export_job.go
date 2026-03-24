package domain

import "time"

// ExportJobStatus tracks the lifecycle of an export request.
type ExportJobStatus string

const (
	ExportStatusPending    ExportJobStatus = "pending"
	ExportStatusRunning    ExportJobStatus = "running"
	ExportStatusCompleted  ExportJobStatus = "completed"
	ExportStatusFailed     ExportJobStatus = "failed"
)

// ExportJob describes a request to pull a subset of logs from S3,
// filter them, and forward the results to OpenSearch for indexing.
type ExportJob struct {
	// ID uniquely identifies the job (UUID).
	ID string `json:"id"`

	// Status is the current lifecycle state.
	Status ExportJobStatus `json:"status"`

	// --- Selection criteria ---

	// StartTime / EndTime define the time window to scan.
	StartTime time.Time `json:"start_time"`
	EndTime   time.Time `json:"end_time"`

	// Service restricts the export to a single service (empty = all).
	Service string `json:"service,omitempty"`

	// LabelFilters are key=value pairs that a log entry must match.
	LabelFilters map[string]string `json:"label_filters,omitempty"`

	// --- Progress / results ---

	ChunksTotal     int       `json:"chunks_total"`
	ChunksProcessed int       `json:"chunks_processed"`
	LogsExported    int       `json:"logs_exported"`
	Error           string    `json:"error,omitempty"`
	CreatedAt       time.Time `json:"created_at"`
	CompletedAt     time.Time `json:"completed_at,omitempty"`
}
