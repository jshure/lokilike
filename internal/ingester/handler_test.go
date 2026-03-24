package ingester

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/joel-shure/lokilike/internal/domain"
)

func TestHandler_PostSuccess(t *testing.T) {
	mock := &mockFlusher{}
	buf := newTestBuffer(10*1024*1024, time.Hour, mock)
	defer buf.Stop()
	handler := NewHandler(buf, 10000)

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
	buf := newTestBuffer(10*1024*1024, time.Hour, mock)
	defer buf.Stop()
	handler := NewHandler(buf, 10000)

	req := httptest.NewRequest(http.MethodGet, "/logs", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}

func TestHandler_InvalidJSON(t *testing.T) {
	mock := &mockFlusher{}
	buf := newTestBuffer(10*1024*1024, time.Hour, mock)
	defer buf.Stop()
	handler := NewHandler(buf, 10000)

	req := httptest.NewRequest(http.MethodPost, "/logs", bytes.NewBufferString("{bad"))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestHandler_EmptyEntries(t *testing.T) {
	mock := &mockFlusher{}
	buf := newTestBuffer(10*1024*1024, time.Hour, mock)
	defer buf.Stop()
	handler := NewHandler(buf, 10000)

	req := httptest.NewRequest(http.MethodPost, "/logs", bytes.NewBufferString(`{"entries":[]}`))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestHandler_TooManyEntries(t *testing.T) {
	mock := &mockFlusher{}
	buf := newTestBuffer(10*1024*1024, time.Hour, mock)
	defer buf.Stop()
	handler := NewHandler(buf, 2) // max 2 entries per request

	body := `{"entries":[
		{"service":"svc","level":"info","message":"a"},
		{"service":"svc","level":"info","message":"b"},
		{"service":"svc","level":"info","message":"c"}
	]}`
	req := httptest.NewRequest(http.MethodPost, "/logs", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", w.Code)
	}
}

func TestHandler_DefaultTimestamp(t *testing.T) {
	mock := &mockFlusher{}
	buf := newTestBuffer(10*1024*1024, time.Hour, mock)
	handler := NewHandler(buf, 10000)

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

	chunk := mock.lastChunk()
	if chunk.MinTime.Before(before) || chunk.MinTime.After(after) {
		t.Errorf("server-assigned timestamp %v not in [%v, %v]", chunk.MinTime, before, after)
	}
}

func TestHandler_BatchFlush(t *testing.T) {
	mock := &mockFlusher{}
	buf := newTestBuffer(100, time.Hour, mock)
	defer buf.Stop()
	handler := NewHandler(buf, 10000)

	type entry struct {
		Service string `json:"service"`
		Level   string `json:"level"`
		Message string `json:"message"`
	}
	type logReq struct {
		Entries []entry `json:"entries"`
	}
	longMsg := string(bytes.Repeat([]byte("x"), 80))
	reqBody, _ := json.Marshal(logReq{Entries: []entry{
		{Service: "svc", Level: "info", Message: longMsg},
		{Service: "svc", Level: "info", Message: longMsg},
		{Service: "svc", Level: "info", Message: longMsg},
	}})

	r := httptest.NewRequest(http.MethodPost, "/logs", bytes.NewReader(reqBody))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", w.Code)
	}
	if mock.flushCount() == 0 {
		t.Fatal("expected at least one flush from small buffer")
	}
}

func TestHandler_MinLevel_DropsEntries(t *testing.T) {
	mock := &mockFlusher{}
	buf := NewBuffer(BufferOpts{
		MaxBytes:    10 * 1024 * 1024,
		MaxAge:      time.Hour,
		Flusher:     mock,
		Compression: domain.CompressionGzip,
		MinLevel:    "error",
	})
	handler := NewHandler(buf, 10000)

	body := `{"entries":[
		{"service":"svc","level":"debug","message":"dropped"},
		{"service":"svc","level":"info","message":"dropped"},
		{"service":"svc","level":"error","message":"kept"}
	]}`
	req := httptest.NewRequest(http.MethodPost, "/logs", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", w.Code)
	}

	var resp map[string]int
	json.NewDecoder(w.Body).Decode(&resp)
	// Only 1 entry should be accepted (the error).
	if resp["accepted"] != 1 {
		t.Fatalf("expected accepted=1, got %d", resp["accepted"])
	}

	buf.Stop()
}
