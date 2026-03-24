package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Ingester metrics.
var (
	EntriesReceived = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sigyn_entries_received_total",
		Help: "Total log entries received by the ingester.",
	})
	EntriesDropped = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sigyn_entries_dropped_total",
		Help: "Log entries dropped, by reason.",
	}, []string{"reason"})
	EntriesBuffered = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "sigyn_entries_buffered",
		Help: "Current entries in the in-memory buffer.",
	})
	ChunksFlushed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sigyn_chunks_flushed_total",
		Help: "Total chunks flushed to S3.",
	})
	FlushErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sigyn_flush_errors_total",
		Help: "Total S3 flush failures.",
	})
	FlushDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "sigyn_flush_duration_seconds",
		Help:    "Time to compress and upload a chunk to S3.",
		Buckets: prometheus.DefBuckets,
	})
	BytesFlushed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sigyn_bytes_flushed_total",
		Help: "Total compressed bytes written to S3.",
	})
)

// WAL metrics.
var (
	WALEntries = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "sigyn_wal_entries",
		Help: "Entries currently in the write-ahead log.",
	})
	WALRecovered = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sigyn_wal_recovered_total",
		Help: "Entries recovered from WAL on startup.",
	})
)

// S3 metrics.
var (
	S3Operations = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sigyn_s3_operations_total",
		Help: "S3 API calls by operation and result.",
	}, []string{"operation", "status"})
	S3Duration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "sigyn_s3_duration_seconds",
		Help:    "S3 operation latency.",
		Buckets: prometheus.DefBuckets,
	}, []string{"operation"})
)

// Exporter metrics.
var (
	ExportJobsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sigyn_export_jobs_total",
		Help: "Export jobs by final status.",
	}, []string{"status"})
	ExportLogsIndexed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sigyn_export_logs_indexed_total",
		Help: "Logs forwarded to OpenSearch via export.",
	})
	BulkIndexDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "sigyn_bulk_index_duration_seconds",
		Help:    "OpenSearch bulk-index latency.",
		Buckets: prometheus.DefBuckets,
	})
)

// HTTP metrics.
var (
	HTTPRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sigyn_http_requests_total",
		Help: "HTTP requests by method, path, and status code.",
	}, []string{"method", "path", "status"})
	HTTPDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "sigyn_http_duration_seconds",
		Help:    "HTTP request latency.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "path"})
)
