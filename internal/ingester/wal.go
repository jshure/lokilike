package ingester

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/joel-shure/lokilike/internal/domain"
	"github.com/joel-shure/lokilike/internal/metrics"
)

// WAL is a write-ahead log that persists buffered entries to disk so they
// survive process crashes. Entries are appended as newline-delimited JSON.
// After a successful S3 flush the WAL is truncated.
//
// Concurrency: all methods are called while the Buffer mutex is held,
// so no additional locking is needed.
type WAL struct {
	file *os.File
	enc  *json.Encoder
	n    int // entries written since last reset
}

// OpenWAL opens (or creates) the WAL file inside dir.
func OpenWAL(dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("wal mkdir %s: %w", dir, err)
	}
	path := filepath.Join(dir, "wal.ndjson")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal open %s: %w", path, err)
	}
	return &WAL{file: f, enc: json.NewEncoder(f)}, nil
}

// Append writes a single entry to the WAL and fsyncs.
func (w *WAL) Append(entry domain.LogEntry) error {
	if err := w.enc.Encode(entry); err != nil {
		return fmt.Errorf("wal encode: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal sync: %w", err)
	}
	w.n++
	metrics.WALEntries.Set(float64(w.n))
	return nil
}

// Reset truncates the WAL after a successful flush.
func (w *WAL) Reset() error {
	if err := w.file.Truncate(0); err != nil {
		return fmt.Errorf("wal truncate: %w", err)
	}
	if _, err := w.file.Seek(0, 0); err != nil {
		return fmt.Errorf("wal seek: %w", err)
	}
	w.enc = json.NewEncoder(w.file)
	w.n = 0
	metrics.WALEntries.Set(0)
	return nil
}

// Recover reads all entries from the WAL. Called once on startup.
func (w *WAL) Recover() ([]domain.LogEntry, error) {
	if _, err := w.file.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("wal seek: %w", err)
	}
	var entries []domain.LogEntry
	scanner := bufio.NewScanner(w.file)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	for scanner.Scan() {
		var e domain.LogEntry
		if err := json.Unmarshal(scanner.Bytes(), &e); err != nil {
			slog.Warn("wal: skipping malformed entry", "error", err)
			continue
		}
		entries = append(entries, e)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("wal scan: %w", err)
	}

	if len(entries) > 0 {
		slog.Info("wal: recovered entries", "count", len(entries))
		metrics.WALRecovered.Add(float64(len(entries)))
	}

	// Move to end for subsequent appends.
	if _, err := w.file.Seek(0, 2); err != nil {
		return nil, fmt.Errorf("wal seek end: %w", err)
	}
	w.n = len(entries)
	metrics.WALEntries.Set(float64(w.n))
	return entries, nil
}

// Close flushes and closes the WAL file.
func (w *WAL) Close() error {
	return w.file.Close()
}
