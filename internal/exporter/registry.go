package exporter

import (
	"context"
	"sync"

	"github.com/joel-shure/sigyn/internal/domain"
)

// Registry tracks export jobs in memory and provides per-job cancellation.
type Registry struct {
	mu      sync.RWMutex
	jobs    map[string]*domain.ExportJob
	cancels map[string]context.CancelFunc
}

// NewRegistry creates an empty job registry.
func NewRegistry() *Registry {
	return &Registry{
		jobs:    make(map[string]*domain.ExportJob),
		cancels: make(map[string]context.CancelFunc),
	}
}

// Add registers a job and its cancel function.
func (r *Registry) Add(job *domain.ExportJob, cancel context.CancelFunc) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.jobs[job.ID] = job
	r.cancels[job.ID] = cancel
}

// Get returns a job by ID.
func (r *Registry) Get(id string) (*domain.ExportJob, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	j, ok := r.jobs[id]
	return j, ok
}

// Cancel cancels a running job. Returns true if the job existed and was cancelled.
func (r *Registry) Cancel(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	cancel, ok := r.cancels[id]
	if !ok {
		return false
	}
	cancel()
	if job, exists := r.jobs[id]; exists {
		job.Status = domain.ExportStatusFailed
		job.Error = "cancelled by user"
	}
	return true
}

// List returns all tracked jobs (newest first by CreatedAt is left to the caller).
func (r *Registry) List() []*domain.ExportJob {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*domain.ExportJob, 0, len(r.jobs))
	for _, j := range r.jobs {
		out = append(out, j)
	}
	return out
}
