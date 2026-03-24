package exporter

import (
	"context"
	"log/slog"
	"sync"

	"github.com/joel-shure/sigyn/internal/domain"
	"github.com/joel-shure/sigyn/internal/metrics"
)

// Pool bounds the number of export jobs that run concurrently.
type Pool struct {
	exporter *Exporter
	registry *Registry
	sem      chan struct{}
	wg       sync.WaitGroup
}

// NewPool creates a worker pool with the given concurrency limit.
func NewPool(exporter *Exporter, registry *Registry, maxConcurrent int) *Pool {
	if maxConcurrent <= 0 {
		maxConcurrent = 4
	}
	return &Pool{
		exporter: exporter,
		registry: registry,
		sem:      make(chan struct{}, maxConcurrent),
	}
}

// Submit queues a job for execution. Returns false if the pool is full.
func (p *Pool) Submit(ctx context.Context, job *domain.ExportJob) bool {
	select {
	case p.sem <- struct{}{}:
		// acquired slot
	default:
		return false // pool full
	}

	jobCtx, cancel := context.WithCancel(ctx)
	p.registry.Add(job, cancel)

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer func() { <-p.sem }()

		if err := p.exporter.Run(jobCtx, job); err != nil {
			slog.Error("export job failed", "job_id", job.ID, "error", err)
			metrics.ExportJobsTotal.WithLabelValues("failed").Inc()
		} else {
			metrics.ExportJobsTotal.WithLabelValues("completed").Inc()
		}
	}()
	return true
}

// Stop waits for all in-flight jobs to finish.
func (p *Pool) Stop() {
	p.wg.Wait()
}
