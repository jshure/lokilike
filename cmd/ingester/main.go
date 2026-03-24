package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joel-shure/lokilike/internal/config"
	"github.com/joel-shure/lokilike/internal/ingester"
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
	log.Debug("ingester config: batch_size=%d, window=%ds, compression=%s",
		cfg.Ingester.BatchSizeBytes, cfg.Ingester.BatchTimeWindowSec, cfg.Ingester.CompressionAlgo)
	log.Debug("s3 config: bucket=%s, prefix=%s, region=%s",
		cfg.Storage.S3.Bucket, cfg.Storage.S3.Prefix, cfg.Storage.S3.Region)

	ctx := context.Background()

	s3Client, err := storage.NewS3Client(ctx, cfg.Storage.S3)
	if err != nil {
		log.Error("failed to init s3 client: %v", err)
		os.Exit(1)
	}

	flusher := ingester.NewS3Flusher(s3Client)
	maxAge := time.Duration(cfg.Ingester.BatchTimeWindowSec) * time.Second
	buf := ingester.NewBuffer(cfg.Ingester.BatchSizeBytes, maxAge, flusher)

	handler := ingester.NewHandler(buf)

	mux := http.NewServeMux()
	mux.Handle("/logs", handler)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	srv := &http.Server{
		Addr:    cfg.Ingester.ListenAddress,
		Handler: mux,
	}

	// Graceful shutdown.
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Info("shutting down...")
		buf.Stop()
		srv.Shutdown(context.Background())
	}()

	log.Info("ingester listening on %s", cfg.Ingester.ListenAddress)
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		log.Error("http server error: %v", err)
		os.Exit(1)
	}
}
