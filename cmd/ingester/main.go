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

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/joel-shure/sigyn/internal/config"
	"github.com/joel-shure/sigyn/internal/domain"
	"github.com/joel-shure/sigyn/internal/index"
	"github.com/joel-shure/sigyn/internal/ingester"
	"github.com/joel-shure/sigyn/internal/logger"
	"github.com/joel-shure/sigyn/internal/middleware"
	"github.com/joel-shure/sigyn/internal/query"
	"github.com/joel-shure/sigyn/internal/storage"
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

	ctx := context.Background()

	// Tenant-aware S3 prefix.
	s3Cfg := cfg.Storage.S3
	if cfg.TenantID != "" {
		s3Cfg.Prefix = s3Cfg.Prefix + cfg.TenantID + "/"
		slog.Info("tenant mode", "tenant_id", cfg.TenantID, "s3_prefix", s3Cfg.Prefix)
	}

	s3Client, err := storage.NewS3Client(ctx, s3Cfg)
	if err != nil {
		slog.Error("failed to init s3 client", "error", err)
		os.Exit(1)
	}

	// Index store for label-based chunk index.
	indexPrefix := cfg.Storage.Index.Prefix
	if cfg.TenantID != "" {
		indexPrefix = indexPrefix + cfg.TenantID + "/"
	}
	idxStore := index.NewStore(s3Client, indexPrefix)

	// WAL setup.
	var wal *ingester.WAL
	if cfg.Ingester.WALDir != "" {
		wal, err = ingester.OpenWAL(cfg.Ingester.WALDir)
		if err != nil {
			slog.Error("failed to open WAL", "error", err)
			os.Exit(1)
		}
		slog.Info("WAL enabled", "dir", cfg.Ingester.WALDir)
	}

	flusher := ingester.NewS3Flusher(s3Client, idxStore)
	maxAge := time.Duration(cfg.Ingester.BatchTimeWindowSec) * time.Second

	algo := domain.CompressionGzip
	if cfg.Ingester.CompressionAlgo == "snappy" {
		algo = domain.CompressionSnappy
	}

	broker := ingester.NewBroker()

	buf := ingester.NewBuffer(ingester.BufferOpts{
		MaxBytes:    cfg.Ingester.BatchSizeBytes,
		MaxAge:      maxAge,
		Flusher:     flusher,
		WAL:         wal,
		Compression: algo,
		MinLevel:    cfg.Ingester.MinLevel,
		Broker:      broker,
	})

	handler := ingester.NewHandler(buf, cfg.Ingester.MaxEntriesPerReq)

	mux := http.NewServeMux()
	mux.Handle("/logs", handler)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("/tail", tailHandler(broker))

	// Build middleware chain.
	var mws []func(http.Handler) http.Handler
	mws = append(mws, middleware.AccessLog)
	mws = append(mws, middleware.BodyLimit(cfg.Ingester.MaxBodyBytes))
	if cfg.Ingester.RateLimitRPS > 0 {
		burst := int(cfg.Ingester.RateLimitRPS)
		if burst < 1 {
			burst = 1
		}
		mws = append(mws, middleware.RateLimit(cfg.Ingester.RateLimitRPS, burst))
		slog.Info("rate limiting enabled", "rps", cfg.Ingester.RateLimitRPS)
	}
	if cfg.Auth.Enabled {
		mws = append(mws, middleware.APIKeyAuth(cfg.Auth.APIKeys))
		slog.Info("API key auth enabled", "keys", len(cfg.Auth.APIKeys))
	}

	srv := &http.Server{
		Addr:    cfg.Ingester.ListenAddress,
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

	// Graceful shutdown.
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		slog.Info("shutting down ingester...")
		buf.Stop()
		srv.Shutdown(context.Background())
	}()

	slog.Info("ingester listening", "address", cfg.Ingester.ListenAddress)

	if cfg.Ingester.TLS.Enabled {
		err = srv.ListenAndServeTLS(cfg.Ingester.TLS.CertFile, cfg.Ingester.TLS.KeyFile)
	} else {
		err = srv.ListenAndServe()
	}
	if err != http.ErrServerClosed {
		slog.Error("http server error", "error", err)
		os.Exit(1)
	}
}

// tailHandler serves a WebSocket that streams live log entries.
// Query params: query={app="nginx"}&level=error
func tailHandler(broker *ingester.Broker) http.HandlerFunc {
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			slog.Error("websocket upgrade failed", "error", err)
			return
		}
		defer conn.Close()

		// Parse filter from query params.
		labelQuery, _ := query.ParseLabelSelector(r.URL.Query().Get("query"))
		levelFilter := strings.ToLower(r.URL.Query().Get("level"))

		filter := func(e domain.LogEntry) bool {
			if levelFilter != "" && !strings.EqualFold(e.Level, levelFilter) {
				return false
			}
			for k, v := range labelQuery {
				if e.Labels[k] != v {
					return false
				}
			}
			return true
		}

		sub := broker.Subscribe(filter)
		defer broker.Unsubscribe(sub.ID)

		slog.Info("tail subscriber connected", "id", sub.ID, "labels", labelQuery, "level", levelFilter)

		// Read pump: detect client disconnect.
		go func() {
			for {
				if _, _, err := conn.ReadMessage(); err != nil {
					break
				}
			}
		}()

		for entry := range sub.Ch {
			data, _ := json.Marshal(entry)
			if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
				break
			}
		}

		slog.Info("tail subscriber disconnected", "id", sub.ID)
	}
}
