package ingester

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/joel-shure/lokilike/internal/domain"
)

// mockFlusher captures flush calls for assertions.
type mockFlusher struct {
	mu     sync.Mutex
	chunks []domain.Chunk
	data   [][]byte
}

func (m *mockFlusher) Flush(chunk domain.Chunk, compressed []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]byte, len(compressed))
	copy(cp, compressed)
	m.chunks = append(m.chunks, chunk)
	m.data = append(m.data, cp)
	return nil
}

func (m *mockFlusher) flushCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.chunks)
}

func (m *mockFlusher) lastChunk() domain.Chunk {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.chunks[len(m.chunks)-1]
}

func (m *mockFlusher) lastData() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.data[len(m.data)-1]
}

func makeEntry(service, msg string, ts time.Time) domain.LogEntry {
	return domain.LogEntry{
		Timestamp: ts,
		Service:   service,
		Level:     "info",
		Message:   msg,
	}
}

func TestCompressEntries_RoundTrip(t *testing.T) {
	entries := []domain.LogEntry{
		makeEntry("svc", "hello", time.Now()),
		makeEntry("svc", "world", time.Now()),
	}

	compressed, err := compressEntries(entries)
	if err != nil {
		t.Fatalf("compressEntries: %v", err)
	}

	// Decompress and verify.
	gz, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		t.Fatalf("gzip reader: %v", err)
	}
	defer gz.Close()

	var got []domain.LogEntry
	scanner := bufio.NewScanner(gz)
	for scanner.Scan() {
		var e domain.LogEntry
		if err := json.Unmarshal(scanner.Bytes(), &e); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		got = append(got, e)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scanner: %v", err)
	}

	if len(got) != len(entries) {
		t.Fatalf("expected %d entries, got %d", len(entries), len(got))
	}
	if got[0].Message != "hello" || got[1].Message != "world" {
		t.Fatalf("unexpected messages: %v", got)
	}
}

func TestBuffer_FlushOnSize(t *testing.T) {
	mock := &mockFlusher{}
	// Tiny maxBytes to force a flush after a couple of entries.
	buf := NewBuffer(200, time.Hour, mock)
	defer buf.Stop()

	now := time.Now().UTC()
	for i := 0; i < 10; i++ {
		buf.Add(makeEntry("myapp", "line", now))
	}

	if mock.flushCount() == 0 {
		t.Fatal("expected at least one flush on size threshold")
	}
}

func TestBuffer_FlushOnTime(t *testing.T) {
	mock := &mockFlusher{}
	buf := NewBuffer(10*1024*1024, 50*time.Millisecond, mock)
	defer buf.Stop()

	buf.Add(makeEntry("svc", "tick", time.Now()))

	// Wait for the timer to fire.
	time.Sleep(200 * time.Millisecond)

	if mock.flushCount() != 1 {
		t.Fatalf("expected 1 flush from timer, got %d", mock.flushCount())
	}
}

func TestBuffer_StopFlushesRemaining(t *testing.T) {
	mock := &mockFlusher{}
	buf := NewBuffer(10*1024*1024, time.Hour, mock)

	buf.Add(makeEntry("svc", "pending", time.Now()))
	buf.Stop()

	if mock.flushCount() != 1 {
		t.Fatalf("expected Stop() to flush remaining entries, got %d flushes", mock.flushCount())
	}
	if mock.lastChunk().EntryCount != 1 {
		t.Fatalf("expected 1 entry in flushed chunk, got %d", mock.lastChunk().EntryCount)
	}
}

func TestBuffer_MinMaxTime(t *testing.T) {
	mock := &mockFlusher{}
	buf := NewBuffer(10*1024*1024, time.Hour, mock)

	t1 := time.Date(2026, 3, 23, 10, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 3, 23, 10, 5, 0, 0, time.UTC)
	t3 := time.Date(2026, 3, 23, 9, 55, 0, 0, time.UTC) // earliest

	buf.Add(makeEntry("svc", "a", t1))
	buf.Add(makeEntry("svc", "b", t2))
	buf.Add(makeEntry("svc", "c", t3))
	buf.Stop()

	chunk := mock.lastChunk()
	if !chunk.MinTime.Equal(t3) {
		t.Errorf("MinTime = %v, want %v", chunk.MinTime, t3)
	}
	if !chunk.MaxTime.Equal(t2) {
		t.Errorf("MaxTime = %v, want %v", chunk.MaxTime, t2)
	}
}

func TestChunkKey_Format(t *testing.T) {
	ts := time.Date(2026, 3, 23, 14, 30, 0, 0, time.UTC)
	key := chunkKey("myapp", ts, "test-uuid")

	if !strings.HasPrefix(key, "myapp/2026/03/23/") {
		t.Errorf("key prefix wrong: %s", key)
	}
	if !strings.HasSuffix(key, "-test-uuid.gz") {
		t.Errorf("key suffix wrong: %s", key)
	}
}
