package ingester

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/joel-shure/sigyn/internal/domain"
	"github.com/joel-shure/sigyn/internal/metrics"
)

// Handler exposes the HTTP ingestion endpoint.
type Handler struct {
	buffer           *Buffer
	maxEntriesPerReq int
}

// NewHandler creates the ingestion HTTP handler.
func NewHandler(buffer *Buffer, maxEntriesPerReq int) *Handler {
	if maxEntriesPerReq <= 0 {
		maxEntriesPerReq = 10000
	}
	return &Handler{buffer: buffer, maxEntriesPerReq: maxEntriesPerReq}
}

// logRequest is the JSON body sent by clients.
type logRequest struct {
	Entries []domain.LogEntry `json:"entries"`
}

// ServeHTTP handles POST /logs.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req logRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json: "+err.Error(), http.StatusBadRequest)
		return
	}

	if len(req.Entries) == 0 {
		http.Error(w, "entries array is empty", http.StatusBadRequest)
		return
	}

	if len(req.Entries) > h.maxEntriesPerReq {
		http.Error(w, "too many entries in single request", http.StatusRequestEntityTooLarge)
		return
	}

	slog.Debug("received log entries", "count", len(req.Entries))

	now := time.Now().UTC()
	accepted := 0
	for i := range req.Entries {
		if req.Entries[i].Timestamp.IsZero() {
			req.Entries[i].Timestamp = now
		}
		if h.buffer.Add(req.Entries[i]) {
			accepted++
		}
	}

	metrics.EntriesReceived.Add(float64(accepted))

	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]int{"accepted": accepted})
}
