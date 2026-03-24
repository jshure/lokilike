package ingester

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/joel-shure/lokilike/internal/domain"
	"github.com/joel-shure/lokilike/internal/logger"
)

// Flusher is called when a batch is ready to be persisted.
type Flusher interface {
	Flush(chunk domain.Chunk, compressed []byte) error
}

// Buffer accumulates LogEntry records in memory and flushes them when
// the batch hits a size threshold or a time window elapses.
type Buffer struct {
	mu       sync.Mutex
	entries  []domain.LogEntry
	sizeEst  int // rough byte estimate of buffered entries
	minTime  time.Time
	maxTime  time.Time
	service  string // current batch service (simplified: one service per batch)

	maxBytes    int
	maxAge      time.Duration
	flusher     Flusher
	flushTicker *time.Ticker
	done        chan struct{}
}

// NewBuffer creates a buffer that flushes on size or time, whichever comes first.
func NewBuffer(maxBytes int, maxAge time.Duration, flusher Flusher) *Buffer {
	b := &Buffer{
		maxBytes:    maxBytes,
		maxAge:      maxAge,
		flusher:     flusher,
		flushTicker: time.NewTicker(maxAge),
		done:        make(chan struct{}),
	}
	go b.tickLoop()
	return b
}

func (b *Buffer) tickLoop() {
	for {
		select {
		case <-b.flushTicker.C:
			b.mu.Lock()
			if len(b.entries) > 0 {
				logger.Get().Debug("flush ticker fired, %d entries buffered", len(b.entries))
				b.flushLocked()
			}
			b.mu.Unlock()
		case <-b.done:
			return
		}
	}
}

// Add appends a log entry to the buffer. If the buffer is full it flushes first.
func (b *Buffer) Add(entry domain.LogEntry) {
	b.mu.Lock()
	defer b.mu.Unlock()

	raw, _ := json.Marshal(entry)
	entrySize := len(raw)

	// If adding this entry would exceed the limit, flush the current batch first.
	if b.sizeEst+entrySize > b.maxBytes && len(b.entries) > 0 {
		logger.Get().Debug("buffer size %d + %d exceeds max %d, flushing", b.sizeEst, entrySize, b.maxBytes)
		b.flushLocked()
	}

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
	b.sizeEst += entrySize
}

// flushLocked compresses and ships the current batch. Caller must hold mu.
func (b *Buffer) flushLocked() {
	log := logger.Get()

	compressed, err := compressEntries(b.entries)
	if err != nil {
		log.Error("compressing batch: %v", err)
		return
	}

	chunk := domain.Chunk{
		Key:         chunkKey(b.service, b.minTime, uuid.NewString()),
		Service:     b.service,
		MinTime:     b.minTime,
		MaxTime:     b.maxTime,
		EntryCount:  len(b.entries),
		SizeBytes:   int64(len(compressed)),
		Compression: domain.CompressionGzip,
	}

	log.Debug("writing chunk %s (%d entries, %d raw bytes -> %d compressed)", chunk.Key, chunk.EntryCount, b.sizeEst, chunk.SizeBytes)

	if err := b.flusher.Flush(chunk, compressed); err != nil {
		log.Error("flushing chunk %s: %v", chunk.Key, err)
		return
	}
	log.Info("flushed chunk %s (%d entries, %d bytes)", chunk.Key, chunk.EntryCount, chunk.SizeBytes)

	// Reset buffer state.
	b.entries = nil
	b.sizeEst = 0
	b.flushTicker.Reset(b.maxAge)
}

// Stop flushes any remaining entries and shuts down the timer.
func (b *Buffer) Stop() {
	b.flushTicker.Stop()
	close(b.done)

	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.entries) > 0 {
		logger.Get().Debug("stop: flushing %d remaining entries", len(b.entries))
		b.flushLocked()
	}
}

// compressEntries gzip-compresses a slice of log entries as newline-delimited JSON.
func compressEntries(entries []domain.LogEntry) ([]byte, error) {
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)

	enc := json.NewEncoder(gw)
	for _, e := range entries {
		if err := enc.Encode(e); err != nil {
			gw.Close()
			return nil, fmt.Errorf("json encode: %w", err)
		}
	}
	if err := gw.Close(); err != nil {
		return nil, fmt.Errorf("gzip close: %w", err)
	}
	return buf.Bytes(), nil
}

// chunkKey builds a hierarchical S3 key: <service>/<YYYY>/<MM>/<DD>/<unix>-<uuid>.gz
func chunkKey(service string, t time.Time, id string) string {
	return fmt.Sprintf("%s/%s/%s-%s.gz",
		service,
		t.UTC().Format("2006/01/02"),
		fmt.Sprintf("%d", t.Unix()),
		id,
	)
}
