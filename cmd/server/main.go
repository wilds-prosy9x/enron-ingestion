package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/wilds-prosy9x/enron-ingestion/internal/ingest"
	"github.com/wilds-prosy9x/enron-ingestion/internal/server"
	"github.com/wilds-prosy9x/enron-ingestion/internal/store"
)

const shutdownTimeout = 10 * time.Second

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, stop); err != nil {
		slog.Error("server stopped with error", "error", err)
		os.Exit(1)
	}
}

// config holds the settings read from environment variables.
type config struct {
	addr          string // ADDR
	datasetBucket string // DATASET_BUCKET, required
	datasetKey    string // DATASET_KEY
	dedupMode     ingest.DedupMode
}

// loadConfig reads the settings using getenv, which is os.Getenv outside tests.
func loadConfig(getenv func(string) string) (config, error) {
	get := func(name, def string) string {
		if v := getenv(name); v != "" {
			return v
		}
		return def
	}
	cfg := config{
		addr:          get("ADDR", ":8080"),
		datasetBucket: getenv("DATASET_BUCKET"),
		datasetKey:    get("DATASET_KEY", "enron_mail_20150507.tar.gz"),
		dedupMode:     ingest.DedupMode(get("DEDUP_MODE", string(ingest.DedupContent))),
	}
	if cfg.datasetBucket == "" {
		return config{}, errors.New("DATASET_BUCKET is required, e.g. file:///data or s3://bucket?region=eu-north-1")
	}
	if cfg.dedupMode != ingest.DedupContent && cfg.dedupMode != ingest.DedupMessageID {
		return config{}, fmt.Errorf("DEDUP_MODE must be %q or %q, got %q", ingest.DedupContent, ingest.DedupMessageID, cfg.dedupMode)
	}
	return cfg, nil
}

// run starts the HTTP server and blocks until ctx is cancelled or the server
// fails. stopSignals is called once shutdown begins so that a second signal
// terminates the process immediately instead of waiting for the shutdown.
func run(ctx context.Context, stopSignals func()) error {
	cfg, err := loadConfig(os.Getenv)
	if err != nil {
		return err
	}

	st := store.NewMemory()
	starter := ingest.NewStarter(st, ingest.StarterConfig{
		Source: ingest.ArchiveSource{BucketURL: cfg.datasetBucket, Key: cfg.datasetKey, Mode: cfg.dedupMode},
	})
	// Runs on every return, after the HTTP server has stopped: cancels a run
	// in progress, which is then marked failed, and waits for it.
	defer starter.Stop()

	// A registry of our own instead of the global default, so that only the
	// metrics registered here are served.
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	srv := &http.Server{
		Addr:              cfg.addr,
		Handler:           server.NewHandler(st, starter, reg),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	serveErr := make(chan error, 1)
	go func() {
		slog.Info("starting server", "addr", cfg.addr)
		serveErr <- srv.ListenAndServe()
	}()

	select {
	case err := <-serveErr:
		// ListenAndServe only returns ErrServerClosed after Shutdown, which
		// hasn't been called yet, so any error here is a real failure.
		return fmt.Errorf("listen: %w", err)
	case <-ctx.Done():
	}

	stopSignals()
	slog.Info("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	if err := <-serveErr; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve: %w", err)
	}
	return nil
}
