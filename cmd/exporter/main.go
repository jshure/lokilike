package main

import (
	"context"
	"embed"
	"encoding/json"
	"io/fs"
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
	"github.com/joel-shure/lokilike/internal/index"
	"github.com/joel-shure/lokilike/internal/logger"
	"github.com/joel-shure/lokilike/internal/middleware"
	"github.com/joel-shure/lokilike/internal/query"
	"github.com/joel-shure/lokilike/internal/storage"
)

//go:embed web
var webFS embed.FS

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

	// Index store.
	indexPrefix := cfg.Storage.Index.Prefix
	if cfg.TenantID != "" {
		indexPrefix = indexPrefix + cfg.TenantID + "/"
	}
	idxStore := index.NewStore(s3Client, indexPrefix)

	osClient, err := exporter.NewOSClient(cfg.Exporter.OpenSearch)
	if err != nil {
		slog.Error("failed to init opensearch client", "error", err)
		os.Exit(1)
	}

	exp := exporter.NewExporter(s3Client, osClient, cfg.Exporter.DefaultBatchSize)
	registry := exporter.NewRegistry()
	pool := exporter.NewPool(exp, registry, cfg.Exporter.MaxConcurrentJobs)

	// Query engine.
	queryEngine := query.NewEngine(s3Client, idxStore)

	mux := http.NewServeMux()

	// --- Export endpoints ---
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

	// --- Query endpoint ---
	mux.HandleFunc("/query", func(w http.ResponseWriter, r *http.Request) {
		handleQuery(w, r, queryEngine)
	})

	// --- Web UI ---
	webSub, _ := fs.Sub(webFS, "web")
	mux.Handle("/ui/", http.StripPrefix("/ui/", http.FileServer(http.FS(webSub))))
	mux.HandleFunc("/ui", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/ui/index.html", http.StatusFound)
	})
	mux.HandleFunc("/ui/config", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"ingester_ws_url": cfg.Exporter.IngesterURL,
		})
	})

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	// Middleware chain.
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

	slog.Info("exporter listening", "address", cfg.Exporter.ListenAddress, "ui", "http://"+cfg.Exporter.ListenAddress+"/ui")
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		slog.Error("http server error", "error", err)
		os.Exit(1)
	}
}

// --- Query handler ---

func handleQuery(w http.ResponseWriter, r *http.Request, engine *query.Engine) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	q := r.URL.Query()

	startStr := q.Get("start")
	endStr := q.Get("end")
	if startStr == "" || endStr == "" {
		http.Error(w, "start and end query params required (RFC3339)", http.StatusBadRequest)
		return
	}

	startTime, err := time.Parse(time.RFC3339, startStr)
	if err != nil {
		http.Error(w, "invalid start (RFC3339)", http.StatusBadRequest)
		return
	}
	endTime, err := time.Parse(time.RFC3339, endStr)
	if err != nil {
		http.Error(w, "invalid end (RFC3339)", http.StatusBadRequest)
		return
	}

	labels, err := query.ParseLabelSelector(q.Get("query"))
	if err != nil {
		http.Error(w, "invalid label selector: "+err.Error(), http.StatusBadRequest)
		return
	}

	limit := 200
	if l := q.Get("limit"); l != "" {
		if _, err := time.ParseDuration("0s"); err == nil { // just reuse stdlib
		}
		n := 0
		for _, c := range l {
			if c >= '0' && c <= '9' {
				n = n*10 + int(c-'0')
			}
		}
		if n > 0 && n <= 5000 {
			limit = n
		}
	}

	// Extract service from labels if present (special key).
	service := ""
	if s, ok := labels["service"]; ok {
		service = s
		delete(labels, "service")
	}

	req := query.Request{
		StartTime: startTime,
		EndTime:   endTime,
		Service:   service,
		Labels:    labels,
		Level:     q.Get("level"),
		Limit:     limit,
	}

	resp, err := engine.Run(r.Context(), req)
	if err != nil {
		slog.Error("query failed", "error", err)
		http.Error(w, "query failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// --- Export handlers ---

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
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(registry.List())
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
