package ingester

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/golang/snappy"
	"github.com/google/uuid"
	"github.com/joel-shure/sigyn/internal/domain"
	"github.com/joel-shure/sigyn/internal/metrics"
)

// Flusher is called when a batch is ready to be persisted.
type Flusher interface {
	Flush(chunk domain.Chunk, compressed []byte) error
}

// BufferOpts configures a Buffer.
type BufferOpts struct {
	MaxBytes    int
	MaxAge      time.Duration
	Flusher     Flusher
	WAL         *WAL                // nil = no WAL
	Compression domain.CompressionAlgo
	MinLevel    string              // drop entries below this level (empty = accept all)
	Broker      *Broker             // nil = no live tail publishing
}

// Buffer accumulates LogEntry records in memory and flushes them when
// the batch hits a size threshold or a time window elapses.
type Buffer struct {
	mu       sync.Mutex
	entries  []domain.LogEntry
	sizeEst  int
	minTime  time.Time
	maxTime  time.Time
	service  string

	opts        BufferOpts
	flushTicker *time.Ticker
	done        chan struct{}
	flushCh     chan flushJob     // async flush queue
	flushWg     sync.WaitGroup   // tracks in-flight flushLoop goroutine
}

// flushJob is a snapshot of entries to be flushed asynchronously.
type flushJob struct {
	entries  []domain.LogEntry
	service  string
	minTime  time.Time
	maxTime  time.Time
}

// NewBuffer creates a buffer that flushes on size or time, whichever comes first.
// If opts.WAL is set, entries surviving a crash are replayed into the buffer.
func NewBuffer(opts BufferOpts) *Buffer {
	b := &Buffer{
		opts:        opts,
		flushTicker: time.NewTicker(opts.MaxAge),
		done:        make(chan struct{}),
		flushCh:     make(chan flushJob, 4),
	}

	// Replay WAL entries from a previous crash.
	if opts.WAL != nil {
		recovered, err := opts.WAL.Recover()
		if err != nil {
			slog.Error("wal recovery failed", "error", err)
		}
		for _, e := range recovered {
			b.addToBuffer(e)
		}
	}

	go b.tickLoop()
	b.flushWg.Add(1)
	go b.flushLoop()
	return b
}

func (b *Buffer) tickLoop() {
	for {
		select {
		case <-b.flushTicker.C:
			b.mu.Lock()
			if len(b.entries) > 0 {
				slog.Debug("flush ticker fired", "entries", len(b.entries))
				b.dispatchFlush()
			}
			b.mu.Unlock()
		case <-b.done:
			return
		}
	}
}

// flushLoop processes flush jobs asynchronously in a dedicated goroutine.
func (b *Buffer) flushLoop() {
	defer b.flushWg.Done()
	for job := range b.flushCh {
		b.executeFlush(job)
	}
}

// levelPriority maps log levels to numeric priority for sampling.
var levelPriority = map[string]int{
	"debug": 0, "trace": 0,
	"info": 1,
	"warn": 2, "warning": 2,
	"error": 3,
	"fatal": 4, "critical": 4,
}

// estimateSize returns a cheap byte-size estimate for a log entry without
// marshalling to JSON. Avoids an allocation per Add call.
func estimateSize(e domain.LogEntry) int {
	n := 80 // fixed overhead: timestamp, field names, braces, etc.
	n += len(e.Service) + len(e.Level) + len(e.Message)
	for k, v := range e.Labels {
		n += len(k) + len(v) + 6 // quotes + colon + comma
	}
	return n
}

// Add appends a log entry to the buffer. Returns false if the entry was
// dropped (e.g., below min_level).
func (b *Buffer) Add(entry domain.LogEntry) bool {
	// Level sampling: drop entries below the configured minimum.
	if b.opts.MinLevel != "" {
		minPri := levelPriority[strings.ToLower(b.opts.MinLevel)]
		entryPri := levelPriority[strings.ToLower(entry.Level)]
		if entryPri < minPri {
			metrics.EntriesDropped.WithLabelValues("below_min_level").Inc()
			return false
		}
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// Write to WAL before buffering so entries survive crashes.
	if b.opts.WAL != nil {
		if err := b.opts.WAL.Append(entry); err != nil {
			slog.Error("wal append failed", "error", err)
			// Continue anyway — we still have the entry in memory.
		}
	}

	entrySize := estimateSize(entry)

	if b.sizeEst+entrySize > b.opts.MaxBytes && len(b.entries) > 0 {
		slog.Debug("buffer size exceeded, flushing",
			"current", b.sizeEst, "incoming", entrySize, "max", b.opts.MaxBytes)
		b.dispatchFlush()
	}

	b.addToBuffer(entry)
	b.sizeEst += entrySize
	metrics.EntriesBuffered.Set(float64(len(b.entries)))

	// Publish to live-tail subscribers (non-blocking).
	if b.opts.Broker != nil {
		b.opts.Broker.Publish(entry)
	}

	return true
}

func (b *Buffer) addToBuffer(entry domain.LogEntry) {
	if len(b.entries) == 0 {
		b.minTime = entry.Timestamp
		b.maxTime = entry.Timestamp
		b.service = entry.Service
	}
	if entry.Timestamp.Before(b.minTime) {
		b.minTime = entry.Timestamp
	}
	if entry.Timestamp.After(b.maxTime) {
		b.maxTime = entry.Timestamp
	}
	b.entries = append(b.entries, entry)
}

// dispatchFlush snapshots the current buffer and sends it to the async flush
// goroutine. The buffer is cleared immediately so Add() returns fast.
// Caller must hold mu.
func (b *Buffer) dispatchFlush() {
	job := flushJob{
		entries: b.entries,
		service: b.service,
		minTime: b.minTime,
		maxTime: b.maxTime,
	}

	// Clear buffer state immediately — entries are owned by the job now.
	b.entries = nil
	b.sizeEst = 0
	metrics.EntriesBuffered.Set(0)
	b.flushTicker.Reset(b.opts.MaxAge)

	select {
	case b.flushCh <- job:
	default:
		// Queue full — flush synchronously to apply backpressure.
		slog.Warn("flush queue full, flushing synchronously", "entries", len(job.entries))
		go b.executeFlush(job)
	}
}

// executeFlush compresses and ships a batch to S3. Runs in the flushLoop goroutine.
func (b *Buffer) executeFlush(job flushJob) {
	start := time.Now()

	compressed, err := compressEntries(job.entries, b.opts.Compression)
	if err != nil {
		slog.Error("compression failed", "error", err)
		metrics.FlushErrors.Inc()
		return
	}

	chunk := domain.Chunk{
		Key:         chunkKey(job.service, job.minTime, uuid.NewString(), b.opts.Compression),
		Service:     job.service,
		MinTime:     job.minTime,
		MaxTime:     job.maxTime,
		EntryCount:  len(job.entries),
		SizeBytes:   int64(len(compressed)),
		Compression: b.opts.Compression,
		LabelSets:   collectLabelSets(job.entries),
	}

	if err := b.opts.Flusher.Flush(chunk, compressed); err != nil {
		slog.Error("flush failed", "key", chunk.Key, "error", err)
		metrics.FlushErrors.Inc()
		return
	}

	dur := time.Since(start)
	slog.Info("flushed chunk",
		"key", chunk.Key, "entries", chunk.EntryCount,
		"bytes", chunk.SizeBytes, "duration_ms", dur.Milliseconds())
	metrics.ChunksFlushed.Inc()
	metrics.BytesFlushed.Add(float64(chunk.SizeBytes))
	metrics.FlushDuration.Observe(dur.Seconds())
}

// Stop flushes any remaining entries, drains the flush queue, and shuts down.
func (b *Buffer) Stop() {
	b.flushTicker.Stop()
	close(b.done)

	b.mu.Lock()
	if len(b.entries) > 0 {
		slog.Debug("stop: flushing remaining entries", "count", len(b.entries))
		b.dispatchFlush()
	}
	b.mu.Unlock()

	// Close the channel and wait for flushLoop to drain all pending jobs.
	close(b.flushCh)
	b.flushWg.Wait()

	// Truncate WAL now that all flushes have completed.
	if b.opts.WAL != nil {
		if err := b.opts.WAL.Reset(); err != nil {
			slog.Error("wal reset on stop failed", "error", err)
		}
		b.opts.WAL.Close()
	}
}

// compressEntries encodes entries as NDJSON and compresses with the given algorithm.
func compressEntries(entries []domain.LogEntry, algo domain.CompressionAlgo) ([]byte, error) {
	// First encode to NDJSON.
	var raw bytes.Buffer
	enc := json.NewEncoder(&raw)
	for _, e := range entries {
		if err := enc.Encode(e); err != nil {
			return nil, fmt.Errorf("json encode: %w", err)
		}
	}

	switch algo {
	case domain.CompressionSnappy:
		return snappy.Encode(nil, raw.Bytes()), nil
	default: // gzip
		var buf bytes.Buffer
		gw := gzip.NewWriter(&buf)
		if _, err := gw.Write(raw.Bytes()); err != nil {
			gw.Close()
			return nil, fmt.Errorf("gzip write: %w", err)
		}
		if err := gw.Close(); err != nil {
			return nil, fmt.Errorf("gzip close: %w", err)
		}
		return buf.Bytes(), nil
	}
}

// collectLabelSets extracts the unique label combinations from a batch of entries.
func collectLabelSets(entries []domain.LogEntry) []map[string]string {
	type key string
	seen := make(map[key]map[string]string)
	for _, e := range entries {
		if len(e.Labels) == 0 {
			continue
		}
		// Build a stable string key for dedup.
		var sb strings.Builder
		for k, v := range e.Labels {
			sb.WriteString(k)
			sb.WriteByte('=')
			sb.WriteString(v)
			sb.WriteByte(',')
		}
		k := key(sb.String())
		if _, ok := seen[k]; !ok {
			cp := make(map[string]string, len(e.Labels))
			for lk, lv := range e.Labels {
				cp[lk] = lv
			}
			seen[k] = cp
		}
	}
	sets := make([]map[string]string, 0, len(seen))
	for _, ls := range seen {
		sets = append(sets, ls)
	}
	return sets
}

// chunkKey builds a hierarchical S3 key: <service>/<YYYY>/<MM>/<DD>/<unix>-<uuid><ext>
func chunkKey(service string, t time.Time, id string, algo domain.CompressionAlgo) string {
	return fmt.Sprintf("%s/%s/%d-%s%s",
		service,
		t.UTC().Format("2006/01/02"),
		t.Unix(),
		id,
		algo.FileExtension(),
	)
}
