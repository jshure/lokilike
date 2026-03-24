package query

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"time"

	"github.com/golang/snappy"
	"github.com/joel-shure/sigyn/internal/domain"
	"github.com/joel-shure/sigyn/internal/index"
	"github.com/joel-shure/sigyn/internal/storage"
)

// Request describes a log query.
type Request struct {
	StartTime time.Time
	EndTime   time.Time
	Service   string
	Labels    map[string]string
	Level     string
	Limit     int
}

// Response is the query result.
type Response struct {
	Entries []domain.LogEntry `json:"entries"`
	Stats   Stats             `json:"stats"`
}

// Stats captures query performance.
type Stats struct {
	IndexFilesScanned int   `json:"index_files_scanned"`
	ChunksMatched     int   `json:"chunks_matched"`
	ChunksFetched     int   `json:"chunks_fetched"`
	EntriesScanned    int   `json:"entries_scanned"`
	EntriesMatched    int   `json:"entries_matched"`
	DurationMs        int64 `json:"duration_ms"`
}

// Engine executes log queries using the label index and S3 storage.
type Engine struct {
	s3    *storage.S3Client
	index *index.Store
}

// NewEngine creates a query engine.
func NewEngine(s3 *storage.S3Client, idx *index.Store) *Engine {
	return &Engine{s3: s3, index: idx}
}

// Run executes a query: reads the index, fetches matching chunks, filters entries.
func (e *Engine) Run(ctx context.Context, req Request) (*Response, error) {
	start := time.Now()

	if req.Limit <= 0 {
		req.Limit = 200
	}

	// Step 1: Query the label index.
	indexes, err := e.index.Query(ctx, req.StartTime, req.EndTime, req.Service, req.Labels)
	if err != nil {
		return nil, err
	}

	stats := Stats{
		IndexFilesScanned: len(indexes),
		ChunksMatched:     len(indexes),
	}

	slog.Debug("query: index matched", "chunks", len(indexes))

	// Step 2: Fetch and filter each matching chunk.
	var entries []domain.LogEntry
	for _, idx := range indexes {
		if len(entries) >= req.Limit {
			break
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		chunkEntries, scanned, err := e.fetchAndFilter(ctx, idx.ChunkKey, req)
		if err != nil {
			slog.Warn("query: chunk fetch failed", "key", idx.ChunkKey, "error", err)
			continue
		}

		stats.ChunksFetched++
		stats.EntriesScanned += scanned
		entries = append(entries, chunkEntries...)
	}

	// Apply limit.
	if len(entries) > req.Limit {
		entries = entries[:req.Limit]
	}

	stats.EntriesMatched = len(entries)
	stats.DurationMs = time.Since(start).Milliseconds()

	slog.Info("query: complete",
		"matched", stats.EntriesMatched, "scanned", stats.EntriesScanned,
		"chunks", stats.ChunksFetched, "duration_ms", stats.DurationMs)

	return &Response{Entries: entries, Stats: stats}, nil
}

// fetchAndFilter downloads a chunk, decompresses it, and returns matching entries.
func (e *Engine) fetchAndFilter(ctx context.Context, chunkKey string, req Request) ([]domain.LogEntry, int, error) {
	data, err := e.s3.GetObject(ctx, chunkKey)
	if err != nil {
		return nil, 0, err
	}

	reader, err := decompress(data, chunkKey)
	if err != nil {
		return nil, 0, err
	}

	var matched []domain.LogEntry
	scanned := 0
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	for scanner.Scan() {
		var entry domain.LogEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			continue
		}
		scanned++

		if entry.Timestamp.Before(req.StartTime) || entry.Timestamp.After(req.EndTime) {
			continue
		}
		if req.Service != "" && !strings.EqualFold(entry.Service, req.Service) {
			continue
		}
		if req.Level != "" && !strings.EqualFold(entry.Level, req.Level) {
			continue
		}
		for k, v := range req.Labels {
			if entry.Labels[k] != v {
				goto skip
			}
		}
		matched = append(matched, entry)
	skip:
	}
	return matched, scanned, scanner.Err()
}

func decompress(data []byte, key string) (*bufio.Reader, error) {
	if strings.HasSuffix(key, ".sz") {
		decoded, err := snappy.Decode(nil, data)
		if err != nil {
			return nil, err
		}
		return bufio.NewReader(bytes.NewReader(decoded)), nil
	}
	gz, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	return bufio.NewReader(gz), nil
}
