package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/joel-shure/lokilike/internal/config"
	"github.com/joel-shure/lokilike/internal/domain"
	"github.com/joel-shure/lokilike/internal/exporter"
	"github.com/joel-shure/lokilike/internal/logger"
	"github.com/joel-shure/lokilike/internal/middleware"
	"github.com/joel-shure/lokilike/internal/storage"
)

func main() {
	cfgPath := "config.json"
	if len(os.Args) > 1 {
		cfgPath = os.Args[1]
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		logger.Init(false)
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	logger.Init(cfg.Debug)
	slog.Info("config loaded", "path", cfgPath, "debug", cfg.Debug)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Tenant-aware S3 prefix.
	s3Cfg := cfg.Storage.S3
	if cfg.TenantID != "" {
		s3Cfg.Prefix = s3Cfg.Prefix + cfg.TenantID + "/"
	}

	s3Client, err := storage.NewS3Client(ctx, s3Cfg)
	if err != nil {
		slog.Error("failed to init s3 client", "error", err)
		os.Exit(1)
	}

	osClient, err := exporter.NewOSClient(cfg.Exporter.OpenSearch)
	if err != nil {
		slog.Error("failed to init opensearch client", "error", err)
		os.Exit(1)
	}

	exp := exporter.NewExporter(s3Client, osClient, cfg.Exporter.DefaultBatchSize)
	registry := exporter.NewRegistry()
	pool := exporter.NewPool(exp, registry, cfg.Exporter.MaxConcurrentJobs)

	mux := http.NewServeMux()

	// POST /export — trigger an export job.
	mux.HandleFunc("/export", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			handleCreateExport(w, r, ctx, pool, registry)
		case http.MethodGet:
			handleListExports(w, registry)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	// GET /export/{id} — poll job status.
	// DELETE /export/{id} — cancel a running job.
	mux.HandleFunc("/export/", func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, "/export/")
		if id == "" {
			http.Error(w, "job id required", http.StatusBadRequest)
			return
		}

		switch r.Method {
		case http.MethodGet:
			handleGetExport(w, id, registry)
		case http.MethodDelete:
			handleCancelExport(w, id, registry)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	// Build middleware chain.
	var mws []func(http.Handler) http.Handler
	mws = append(mws, middleware.AccessLog)
	if cfg.Auth.Enabled {
		mws = append(mws, middleware.APIKeyAuth(cfg.Auth.APIKeys))
	}

	srv := &http.Server{
		Addr:    cfg.Exporter.ListenAddress,
		Handler: middleware.Chain(mux, mws...),
	}

	// Prometheus metrics server.
	if cfg.Metrics.Enabled {
		go func() {
			metricsMux := http.NewServeMux()
			metricsMux.Handle("/metrics", promhttp.Handler())
			slog.Info("metrics server listening", "address", cfg.Metrics.Address)
			if err := http.ListenAndServe(cfg.Metrics.Address, metricsMux); err != nil {
				slog.Error("metrics server error", "error", err)
			}
		}()
	}

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		slog.Info("shutting down exporter...")
		cancel()
		pool.Stop()
		srv.Shutdown(context.Background())
	}()

	slog.Info("exporter listening", "address", cfg.Exporter.ListenAddress)
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		slog.Error("http server error", "error", err)
		os.Exit(1)
	}
}

func handleCreateExport(w http.ResponseWriter, r *http.Request, ctx context.Context, pool *exporter.Pool, registry *exporter.Registry) {
	var req struct {
		StartTime    string            `json:"start_time"`
		EndTime      string            `json:"end_time"`
		Service      string            `json:"service"`
		LabelFilters map[string]string `json:"label_filters"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json: "+err.Error(), http.StatusBadRequest)
		return
	}

	startTime, err := time.Parse(time.RFC3339, req.StartTime)
	if err != nil {
		http.Error(w, "invalid start_time (use RFC3339)", http.StatusBadRequest)
		return
	}
	endTime, err := time.Parse(time.RFC3339, req.EndTime)
	if err != nil {
		http.Error(w, "invalid end_time (use RFC3339)", http.StatusBadRequest)
		return
	}

	job := &domain.ExportJob{
		ID:           uuid.NewString(),
		Status:       domain.ExportStatusPending,
		StartTime:    startTime,
		EndTime:      endTime,
		Service:      req.Service,
		LabelFilters: req.LabelFilters,
		CreatedAt:    time.Now().UTC(),
	}

	if !pool.Submit(ctx, job) {
		http.Error(w, "too many concurrent export jobs", http.StatusServiceUnavailable)
		return
	}

	slog.Info("export job created", "job_id", job.ID, "service", job.Service,
		"start", req.StartTime, "end", req.EndTime)

	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(job)
}

func handleGetExport(w http.ResponseWriter, id string, registry *exporter.Registry) {
	job, ok := registry.Get(id)
	if !ok {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(job)
}

func handleListExports(w http.ResponseWriter, registry *exporter.Registry) {
	jobs := registry.List()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(jobs)
}

func handleCancelExport(w http.ResponseWriter, id string, registry *exporter.Registry) {
	if registry.Cancel(id) {
		slog.Info("export job cancelled", "job_id", id)
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "cancelled"})
	} else {
		http.Error(w, "job not found", http.StatusNotFound)
	}
}
