package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/joel-shure/lokilike/internal/config"
	"github.com/joel-shure/lokilike/internal/domain"
	"github.com/joel-shure/lokilike/internal/exporter"
	"github.com/joel-shure/lokilike/internal/logger"
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
		logger.Get().Error("failed to load config: %v", err)
		os.Exit(1)
	}

	logger.Init(cfg.Debug)
	log := logger.Get()

	log.Info("config loaded from %s (debug=%v)", cfgPath, cfg.Debug)
	log.Debug("opensearch config: endpoint=%s, index_prefix=%s",
		cfg.Exporter.OpenSearch.Endpoint, cfg.Exporter.OpenSearch.IndexPrefix)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s3Client, err := storage.NewS3Client(ctx, cfg.Storage.S3)
	if err != nil {
		log.Error("failed to init s3 client: %v", err)
		os.Exit(1)
	}

	osClient, err := exporter.NewOSClient(cfg.Exporter.OpenSearch)
	if err != nil {
		log.Error("failed to init opensearch client: %v", err)
		os.Exit(1)
	}

	exp := exporter.NewExporter(s3Client, osClient, cfg.Exporter.DefaultBatchSize)

	mux := http.NewServeMux()

	// POST /export — trigger an export job.
	mux.HandleFunc("/export", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

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

		log.Info("export job %s created: service=%s range=[%s, %s]",
			job.ID, job.Service, req.StartTime, req.EndTime)

		// Run the export asynchronously.
		go func() {
			if err := exp.Run(ctx, job); err != nil {
				log.Error("export job %s failed: %v", job.ID, err)
			}
		}()

		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(job)
	})

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	srv := &http.Server{
		Addr:    ":8081",
		Handler: mux,
	}

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Info("shutting down exporter...")
		cancel()
		srv.Shutdown(context.Background())
	}()

	log.Info("exporter listening on :8081")
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		log.Error("http server error: %v", err)
		os.Exit(1)
	}
}
