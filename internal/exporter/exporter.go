package exporter

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/joel-shure/lokilike/internal/domain"
	"github.com/joel-shure/lokilike/internal/logger"
	"github.com/joel-shure/lokilike/internal/storage"
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

// Run executes an ExportJob: list relevant S3 keys, fetch each chunk,
// decompress, filter, and bulk-index into OpenSearch.
// It mutates the job's progress fields in place.
func (e *Exporter) Run(ctx context.Context, job *domain.ExportJob) error {
	log := logger.Get()
	job.Status = domain.ExportStatusRunning

	// Build the list of S3 key prefixes to scan based on the time range.
	prefixes := timePrefixes(job.Service, job.StartTime, job.EndTime)
	log.Debug("export job %s: scanning %d day-prefixes", job.ID, len(prefixes))

	var allKeys []string
	for _, prefix := range prefixes {
		keys, err := e.s3.ListObjects(ctx, prefix)
		if err != nil {
			job.Status = domain.ExportStatusFailed
			job.Error = err.Error()
			return err
		}
		log.Debug("export job %s: prefix %s yielded %d keys", job.ID, prefix, len(keys))
		allKeys = append(allKeys, keys...)
	}

	job.ChunksTotal = len(allKeys)
	log.Info("export job %s: found %d chunks to scan", job.ID, len(allKeys))

	var pendingBatch []domain.LogEntry
	totalExported := 0

	for _, key := range allKeys {
		select {
		case <-ctx.Done():
			job.Status = domain.ExportStatusFailed
			job.Error = ctx.Err().Error()
			return ctx.Err()
		default:
		}

		entries, err := e.fetchAndFilter(ctx, key, job)
		if err != nil {
			log.Error("export job %s: error processing %s: %v", job.ID, key, err)
			job.ChunksProcessed++
			continue
		}

		log.Debug("export job %s: chunk %s yielded %d matching entries", job.ID, key, len(entries))
		pendingBatch = append(pendingBatch, entries...)

		// Flush to OpenSearch when we have enough.
		for len(pendingBatch) >= e.batchSize {
			batch := pendingBatch[:e.batchSize]
			pendingBatch = pendingBatch[e.batchSize:]

			indexed, err := e.os.BulkIndex(ctx, batch)
			if err != nil {
				log.Error("export job %s: bulk index error: %v", job.ID, err)
			}
			totalExported += indexed
			log.Debug("export job %s: bulk indexed %d entries", job.ID, indexed)
		}

		job.ChunksProcessed++
	}

	// Flush remaining.
	if len(pendingBatch) > 0 {
		log.Debug("export job %s: flushing final batch of %d entries", job.ID, len(pendingBatch))
		indexed, err := e.os.BulkIndex(ctx, pendingBatch)
		if err != nil {
			log.Error("export job %s: final bulk index error: %v", job.ID, err)
		}
		totalExported += indexed
	}

	job.LogsExported = totalExported
	job.Status = domain.ExportStatusCompleted
	job.CompletedAt = time.Now().UTC()
	log.Info("export job %s: completed, %d logs exported", job.ID, totalExported)
	return nil
}

// fetchAndFilter downloads a single chunk, decompresses it, and returns
// only the entries that match the job criteria.
func (e *Exporter) fetchAndFilter(ctx context.Context, key string, job *domain.ExportJob) ([]domain.LogEntry, error) {
	// The keys from ListObjects already include the prefix.
	data, err := e.s3.GetObjectRaw(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("fetch %s: %w", key, err)
	}

	logger.Get().Debug("decompressing chunk %s (%d bytes)", key, len(data))

	gz, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("gzip open %s: %w", key, err)
	}
	defer gz.Close()

	var matched []domain.LogEntry
	scanner := bufio.NewScanner(gz)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024) // 1MB line buffer
	for scanner.Scan() {
		var entry domain.LogEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			continue // skip malformed lines
		}
		if matchesJob(entry, job) {
			matched = append(matched, entry)
		}
	}
	return matched, scanner.Err()
}

// matchesJob returns true if the entry satisfies the job's filter criteria.
func matchesJob(entry domain.LogEntry, job *domain.ExportJob) bool {
	// Time range filter.
	if entry.Timestamp.Before(job.StartTime) || entry.Timestamp.After(job.EndTime) {
		return false
	}
	// Service filter.
	if job.Service != "" && !strings.EqualFold(entry.Service, job.Service) {
		return false
	}
	// Label filters: every filter key=value must be present on the entry.
	for k, v := range job.LabelFilters {
		if entry.Labels[k] != v {
			return false
		}
	}
	return true
}

// timePrefixes generates the S3 key prefixes we need to scan for a given
// time range. Each prefix covers one calendar day: <service>/YYYY/MM/DD/
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
