package ingester

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/joel-shure/lokilike/internal/domain"
	"github.com/joel-shure/lokilike/internal/logger"
)

// Handler exposes the HTTP ingestion endpoint.
type Handler struct {
	buffer *Buffer
}

// NewHandler creates the ingestion HTTP handler.
func NewHandler(buffer *Buffer) *Handler {
	return &Handler{buffer: buffer}
}

// logRequest is the JSON body sent by clients. It may contain a single
// entry or a batch.
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

	log := logger.Get()
	log.Debug("received %d log entries", len(req.Entries))

	now := time.Now().UTC()
	for i := range req.Entries {
		if req.Entries[i].Timestamp.IsZero() {
			req.Entries[i].Timestamp = now
		}
		h.buffer.Add(req.Entries[i])
	}

	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]int{"accepted": len(req.Entries)})
}
