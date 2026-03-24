package ingester

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHandler_PostSuccess(t *testing.T) {
	mock := &mockFlusher{}
	buf := NewBuffer(10*1024*1024, time.Hour, mock)
	defer buf.Stop()
	handler := NewHandler(buf)

	body := `{"entries":[{"service":"svc","level":"info","message":"hello"}]}`
	req := httptest.NewRequest(http.MethodPost, "/logs", bytes.NewBufferString(body))
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", w.Code)
	}

	var resp map[string]int
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["accepted"] != 1 {
		t.Fatalf("expected accepted=1, got %d", resp["accepted"])
	}
}

func TestHandler_MethodNotAllowed(t *testing.T) {
	mock := &mockFlusher{}
	buf := NewBuffer(10*1024*1024, time.Hour, mock)
	defer buf.Stop()
	handler := NewHandler(buf)

	req := httptest.NewRequest(http.MethodGet, "/logs", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}

func TestHandler_InvalidJSON(t *testing.T) {
	mock := &mockFlusher{}
	buf := NewBuffer(10*1024*1024, time.Hour, mock)
	defer buf.Stop()
	handler := NewHandler(buf)

	req := httptest.NewRequest(http.MethodPost, "/logs", bytes.NewBufferString("{bad"))
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestHandler_EmptyEntries(t *testing.T) {
	mock := &mockFlusher{}
	buf := NewBuffer(10*1024*1024, time.Hour, mock)
	defer buf.Stop()
	handler := NewHandler(buf)

	req := httptest.NewRequest(http.MethodPost, "/logs", bytes.NewBufferString(`{"entries":[]}`))
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestHandler_DefaultTimestamp(t *testing.T) {
	mock := &mockFlusher{}
	buf := NewBuffer(10*1024*1024, time.Hour, mock)
	handler := NewHandler(buf)

	body := `{"entries":[{"service":"svc","level":"info","message":"no-ts"}]}`
	before := time.Now().UTC()
	req := httptest.NewRequest(http.MethodPost, "/logs", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	after := time.Now().UTC()

	buf.Stop()

	if mock.flushCount() != 1 {
		t.Fatalf("expected 1 flush, got %d", mock.flushCount())
	}

	// Decompress and check the timestamp was server-assigned.
	chunk := mock.lastChunk()
	if chunk.MinTime.Before(before) || chunk.MinTime.After(after) {
		t.Errorf("server-assigned timestamp %v not in [%v, %v]", chunk.MinTime, before, after)
	}
}

func TestHandler_BatchFlush(t *testing.T) {
	mock := &mockFlusher{}
	// Very small buffer to trigger flush from handler.
	buf := NewBuffer(100, time.Hour, mock)
	defer buf.Stop()
	handler := NewHandler(buf)

	// Build entries with long messages to exceed the 100-byte buffer.
	type entry struct {
		Service string `json:"service"`
		Level   string `json:"level"`
		Message string `json:"message"`
	}
	type req struct {
		Entries []entry `json:"entries"`
	}
	longMsg := string(bytes.Repeat([]byte("x"), 80))
	body, _ := json.Marshal(req{Entries: []entry{
		{Service: "svc", Level: "info", Message: longMsg},
		{Service: "svc", Level: "info", Message: longMsg},
		{Service: "svc", Level: "info", Message: longMsg},
	}})

	r := httptest.NewRequest(http.MethodPost, "/logs", bytes.NewReader(body))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", w.Code)
	}
	if mock.flushCount() == 0 {
		t.Fatal("expected at least one flush from small buffer")
	}
}
