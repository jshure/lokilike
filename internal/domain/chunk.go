package domain

import "time"

// CompressionAlgo identifies the compression used for a chunk.
type CompressionAlgo string

const (
	CompressionGzip   CompressionAlgo = "gzip"
	CompressionSnappy CompressionAlgo = "snappy"
)

// FileExtension returns the file extension for this compression algorithm.
func (a CompressionAlgo) FileExtension() string {
	switch a {
	case CompressionSnappy:
		return ".sz"
	default:
		return ".gz"
	}
}

// Chunk represents a compressed batch of log entries stored in S3.
type Chunk struct {
	// Key is the full S3 object key.
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

	// LabelSets are the unique label combinations present in this chunk.
	// Used by the label index to skip irrelevant chunks during queries.
	LabelSets []map[string]string `json:"label_sets,omitempty"`
}
