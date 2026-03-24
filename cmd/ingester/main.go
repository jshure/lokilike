package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/joel-shure/lokilike/internal/config"
	"github.com/joel-shure/lokilike/internal/domain"
	"github.com/joel-shure/lokilike/internal/ingester"
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

	flusher := ingester.NewS3Flusher(s3Client)
	maxAge := time.Duration(cfg.Ingester.BatchTimeWindowSec) * time.Second

	algo := domain.CompressionGzip
	if cfg.Ingester.CompressionAlgo == "snappy" {
		algo = domain.CompressionSnappy
	}

	buf := ingester.NewBuffer(ingester.BufferOpts{
		MaxBytes:    cfg.Ingester.BatchSizeBytes,
		MaxAge:      maxAge,
		Flusher:     flusher,
		WAL:         wal,
		Compression: algo,
		MinLevel:    cfg.Ingester.MinLevel,
	})

	handler := ingester.NewHandler(buf, cfg.Ingester.MaxEntriesPerReq)

	mux := http.NewServeMux()
	mux.Handle("/logs", handler)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

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
