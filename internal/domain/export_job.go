package domain

import "time"

// ExportJobStatus tracks the lifecycle of an export request.
type ExportJobStatus string

const (
	ExportStatusPending   ExportJobStatus = "pending"
	ExportStatusRunning   ExportJobStatus = "running"
	ExportStatusCompleted ExportJobStatus = "completed"
	ExportStatusFailed    ExportJobStatus = "failed"
)

// ExportJob describes a request to pull a subset of logs from S3,
// filter them, and forward the results to OpenSearch for indexing.
type ExportJob struct {
	ID     string          `json:"id"`
	Status ExportJobStatus `json:"status"`

	// --- Selection criteria ---

	StartTime    time.Time         `json:"start_time"`
	EndTime      time.Time         `json:"end_time"`
	Service      string            `json:"service,omitempty"`
	LabelFilters map[string]string `json:"label_filters,omitempty"`

	// --- Progress / results ---

	ChunksTotal     int       `json:"chunks_total"`
	ChunksProcessed int       `json:"chunks_processed"`
	LogsExported    int       `json:"logs_exported"`
	ErrorCount      int       `json:"error_count"`
	Error           string    `json:"error,omitempty"`
	CreatedAt       time.Time `json:"created_at"`
	CompletedAt     time.Time `json:"completed_at,omitempty"`
}
