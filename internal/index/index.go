package index

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"time"

	"github.com/joel-shure/lokilike/internal/storage"
)

// ChunkIndex is the minimal metadata written to S3 alongside each chunk.
// It enables the query engine to skip chunks that cannot contain matching entries.
type ChunkIndex struct {
	ChunkKey    string              `json:"chunk_key"`
	Service     string              `json:"service"`
	MinTime     time.Time           `json:"min_time"`
	MaxTime     time.Time           `json:"max_time"`
	LabelSets   []map[string]string `json:"label_sets"`
	EntryCount  int                 `json:"entry_count"`
	SizeBytes   int64               `json:"size_bytes"`
	Compression string              `json:"compression"`
}

// Store reads and writes ChunkIndex files to S3.
type Store struct {
	s3     *storage.S3Client
	prefix string // e.g. "index/" or "index/tenant/"
}

// NewStore creates an index store. prefix is the S3 key prefix for index files.
func NewStore(s3 *storage.S3Client, prefix string) *Store {
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	return &Store{s3: s3, prefix: prefix}
}

// Write persists a ChunkIndex to S3.
func (s *Store) Write(ctx context.Context, idx ChunkIndex) error {
	key := s.indexKey(idx.ChunkKey)
	data, err := json.Marshal(idx)
	if err != nil {
		return fmt.Errorf("marshal index: %w", err)
	}
	slog.Debug("index: writing", "key", key)
	return s.s3.PutObjectRaw(ctx, key, data)
}

// Query returns all ChunkIndex entries that overlap the given time range
// and match the label selector. If service is empty, all services are included.
func (s *Store) Query(ctx context.Context, start, end time.Time, service string, labels map[string]string) ([]ChunkIndex, error) {
	prefixes := s.dayPrefixes(start, end)
	slog.Debug("index: querying", "prefixes", len(prefixes), "service", service, "labels", labels)

	var results []ChunkIndex
	for _, pfx := range prefixes {
		keys, err := s.s3.ListObjectsRaw(ctx, pfx)
		if err != nil {
			return nil, fmt.Errorf("list index %s: %w", pfx, err)
		}

		for _, key := range keys {
			data, err := s.s3.GetObjectRaw(ctx, key)
			if err != nil {
				slog.Warn("index: failed to read", "key", key, "error", err)
				continue
			}

			var idx ChunkIndex
			if err := json.Unmarshal(data, &idx); err != nil {
				slog.Warn("index: malformed", "key", key, "error", err)
				continue
			}

			if !timeOverlaps(idx.MinTime, idx.MaxTime, start, end) {
				continue
			}
			if service != "" && !strings.EqualFold(idx.Service, service) {
				continue
			}
			if !MatchLabels(idx.LabelSets, labels) {
				continue
			}

			results = append(results, idx)
		}
	}

	slog.Debug("index: query done", "matched", len(results))
	return results, nil
}

// MatchLabels returns true if any label set contains all the query labels.
func MatchLabels(labelSets []map[string]string, query map[string]string) bool {
	if len(query) == 0 {
		return true
	}
	if len(labelSets) == 0 {
		return false
	}
	for _, ls := range labelSets {
		allMatch := true
		for k, v := range query {
			if ls[k] != v {
				allMatch = false
				break
			}
		}
		if allMatch {
			return true
		}
	}
	return false
}

func timeOverlaps(idxMin, idxMax, qStart, qEnd time.Time) bool {
	return !idxMax.Before(qStart) && !idxMin.After(qEnd)
}

// indexKey converts a chunk key to an index key.
// chunk: myapp/2026/03/23/12345-uuid.gz → index: <prefix>2026/03/23/myapp-12345-uuid.json
func (s *Store) indexKey(chunkKey string) string {
	parts := strings.SplitN(chunkKey, "/", 2) // ["myapp", "2026/03/23/12345-uuid.gz"]
	service := parts[0]
	rest := chunkKey
	if len(parts) == 2 {
		rest = parts[1]
	}

	ext := filepath.Ext(rest)
	base := strings.TrimSuffix(rest, ext)
	dir := filepath.Dir(base)
	file := filepath.Base(base)

	return fmt.Sprintf("%s%s/%s-%s.json", s.prefix, dir, service, file)
}

// dayPrefixes returns index key prefixes for each day in the range.
func (s *Store) dayPrefixes(start, end time.Time) []string {
	start = start.UTC().Truncate(24 * time.Hour)
	end = end.UTC().Truncate(24 * time.Hour)

	var prefixes []string
	for d := start; !d.After(end); d = d.Add(24 * time.Hour) {
		prefixes = append(prefixes, s.prefix+d.Format("2006/01/02/"))
	}
	return prefixes
}
