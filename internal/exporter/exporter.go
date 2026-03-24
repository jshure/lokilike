package exporter

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/golang/snappy"
	"github.com/joel-shure/sigyn/internal/domain"
	"github.com/joel-shure/sigyn/internal/metrics"
	"github.com/joel-shure/sigyn/internal/retry"
	"github.com/joel-shure/sigyn/internal/storage"
)

// Exporter reads compressed chunks from S3, filters entries by the job
// criteria, and bulk-indexes matching logs into OpenSearch.
type Exporter struct {
	s3        *storage.S3Client
	os        *OSClient
	batchSize int
}

// NewExporter creates an Exporter.
func NewExporter(s3 *storage.S3Client, os *OSClient, batchSize int) *Exporter {
	if batchSize <= 0 {
		batchSize = 1000
	}
	return &Exporter{s3: s3, os: os, batchSize: batchSize}
}

// Run executes an ExportJob end-to-end.
func (e *Exporter) Run(ctx context.Context, job *domain.ExportJob) error {
	job.Status = domain.ExportStatusRunning

	prefixes := timePrefixes(job.Service, job.StartTime, job.EndTime)
	slog.Debug("export: scanning prefixes", "job_id", job.ID, "prefixes", len(prefixes))

	var allKeys []string
	for _, prefix := range prefixes {
		keys, err := e.s3.ListObjects(ctx, prefix)
		if err != nil {
			job.Status = domain.ExportStatusFailed
			job.Error = err.Error()
			return err
		}
		slog.Debug("export: prefix scanned", "job_id", job.ID, "prefix", prefix, "keys", len(keys))
		allKeys = append(allKeys, keys...)
	}

	job.ChunksTotal = len(allKeys)
	slog.Info("export: starting", "job_id", job.ID, "chunks", len(allKeys))

	var pendingBatch []domain.LogEntry
	totalExported := 0

	for _, key := range allKeys {
		if err := ctx.Err(); err != nil {
			job.Status = domain.ExportStatusFailed
			job.Error = err.Error()
			return err
		}

		entries, err := e.fetchAndFilter(ctx, key, job)
		if err != nil {
			slog.Error("export: chunk error", "job_id", job.ID, "key", key, "error", err)
			job.ErrorCount++
			job.ChunksProcessed++
			continue
		}

		slog.Debug("export: chunk filtered", "job_id", job.ID, "key", key, "matched", len(entries))
		pendingBatch = append(pendingBatch, entries...)

		for len(pendingBatch) >= e.batchSize {
			batch := pendingBatch[:e.batchSize]
			pendingBatch = pendingBatch[e.batchSize:]

			indexed, err := e.bulkIndexWithRetry(ctx, job.ID, batch)
			if err != nil {
				job.ErrorCount++
			}
			totalExported += indexed
		}

		job.ChunksProcessed++
	}

	// Flush remaining.
	if len(pendingBatch) > 0 {
		indexed, err := e.bulkIndexWithRetry(ctx, job.ID, pendingBatch)
		if err != nil {
			job.ErrorCount++
		}
		totalExported += indexed
	}

	job.LogsExported = totalExported
	job.CompletedAt = time.Now().UTC()
	metrics.ExportLogsIndexed.Add(float64(totalExported))

	if job.ErrorCount > 0 {
		job.Status = domain.ExportStatusCompleted
		job.Error = fmt.Sprintf("%d errors during export", job.ErrorCount)
	} else {
		job.Status = domain.ExportStatusCompleted
	}
	slog.Info("export: completed", "job_id", job.ID, "exported", totalExported, "errors", job.ErrorCount)
	return nil
}

func (e *Exporter) bulkIndexWithRetry(ctx context.Context, jobID string, batch []domain.LogEntry) (int, error) {
	var indexed int
	err := retry.Do(ctx, retry.DefaultOptions, func() error {
		start := time.Now()
		n, err := e.os.BulkIndex(ctx, batch)
		metrics.BulkIndexDuration.Observe(time.Since(start).Seconds())
		indexed = n
		if err != nil {
			slog.Warn("export: bulk index attempt failed", "job_id", jobID, "error", err)
		}
		return err
	})
	if err != nil {
		slog.Error("export: bulk index failed after retries", "job_id", jobID, "error", err)
	}
	return indexed, err
}

// fetchAndFilter downloads a chunk, decompresses it, and returns matching entries.
func (e *Exporter) fetchAndFilter(ctx context.Context, key string, job *domain.ExportJob) ([]domain.LogEntry, error) {
	data, err := e.s3.GetObjectRaw(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("fetch %s: %w", key, err)
	}

	reader, err := decompress(data, key)
	if err != nil {
		return nil, fmt.Errorf("decompress %s: %w", key, err)
	}

	var matched []domain.LogEntry
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	for scanner.Scan() {
		var entry domain.LogEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			continue
		}
		if matchesJob(entry, job) {
			matched = append(matched, entry)
		}
	}
	return matched, scanner.Err()
}

// decompress detects the compression algorithm from the key extension.
func decompress(data []byte, key string) (*bufio.Reader, error) {
	if strings.HasSuffix(key, ".sz") {
		decoded, err := snappy.Decode(nil, data)
		if err != nil {
			return nil, err
		}
		return bufio.NewReader(bytes.NewReader(decoded)), nil
	}
	// Default to gzip.
	gz, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	return bufio.NewReader(gz), nil
}

// matchesJob returns true if the entry satisfies the job's filter criteria.
func matchesJob(entry domain.LogEntry, job *domain.ExportJob) bool {
	if entry.Timestamp.Before(job.StartTime) || entry.Timestamp.After(job.EndTime) {
		return false
	}
	if job.Service != "" && !strings.EqualFold(entry.Service, job.Service) {
		return false
	}
	for k, v := range job.LabelFilters {
		if entry.Labels[k] != v {
			return false
		}
	}
	return true
}

// timePrefixes generates per-day S3 key prefixes for the time range.
func timePrefixes(service string, start, end time.Time) []string {
	start = start.UTC().Truncate(24 * time.Hour)
	end = end.UTC().Truncate(24 * time.Hour)

	var prefixes []string
	for d := start; !d.After(end); d = d.Add(24 * time.Hour) {
		prefix := d.Format("2006/01/02/")
		if service != "" {
			prefix = service + "/" + prefix
		}
		prefixes = append(prefixes, prefix)
	}
	return prefixes
}
