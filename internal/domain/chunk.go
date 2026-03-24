package domain

import "time"

// CompressionAlgo identifies the compression used for a chunk.
type CompressionAlgo string

const (
	CompressionGzip   CompressionAlgo = "gzip"
	CompressionSnappy CompressionAlgo = "snappy"
)

// Chunk represents a compressed batch of log entries stored in S3.
// The S3 key encodes the service, time range, and a unique ID so the
// exporter can narrow its ListObjects calls by prefix.
type Chunk struct {
	// Key is the full S3 object key (e.g. "raw_logs/myapp/2026/03/23/1679558400-1679558430-abc123.gz").
	Key string `json:"key"`

	// Service groups logs by originating application.
	Service string `json:"service"`

	// MinTime / MaxTime bound the timestamps of entries inside the chunk.
	MinTime time.Time `json:"min_time"`
	MaxTime time.Time `json:"max_time"`

	// EntryCount is the number of LogEntry records in the chunk.
	EntryCount int `json:"entry_count"`

	// SizeBytes is the compressed size written to S3.
	SizeBytes int64 `json:"size_bytes"`

	// Compression indicates the algorithm used.
	Compression CompressionAlgo `json:"compression"`
}
