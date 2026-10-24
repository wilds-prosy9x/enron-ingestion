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

	"github.com/redis/go-redis/v9"

	"github.com/wilds-prosy9x/enron-ingestion/internal/ingest"
	"github.com/wilds-prosy9x/enron-ingestion/internal/server"
	"github.com/wilds-prosy9x/enron-ingestion/internal/store"
)

const (
	shutdownTimeout = 10 * time.Second
	// redisKeyPrefix namespaces the app's keys in Redis.
	redisKeyPrefix = "enron"
)

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
	redisAddr     string // REDIS_ADDR
	redisPassword string // REDIS_PASSWORD
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
		redisAddr:     get("REDIS_ADDR", "localhost:6379"),
		redisPassword: getenv("REDIS_PASSWORD"),
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

	rdb := redis.NewClient(&redis.Options{Addr: cfg.redisAddr, Password: cfg.redisPassword})
	defer rdb.Close()
	// Redis may become available later, so a failed check isn't fatal;
	// requests fail with 500 until it is reachable.
	pingCtx, cancelPing := context.WithTimeout(ctx, 2*time.Second)
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		slog.Warn("redis not reachable", "addr", cfg.redisAddr, "error", err)
	}
	cancelPing()

	st := store.NewRedis(rdb, redisKeyPrefix)
	starter := ingest.NewStarter(st, ingest.StarterConfig{
		Source:    ingest.ArchiveSource{BucketURL: cfg.datasetBucket, Key: cfg.datasetKey, Mode: cfg.dedupMode},
		DedupMode: cfg.dedupMode,
	})
	// Runs on every return, after the HTTP server has stopped: cancels a run
	// in progress, which is then marked failed, and waits for it.
	defer starter.Stop()

	addr := cfg.addr
	srv := &http.Server{
		Addr:              addr,
		Handler:           server.NewHandler(st, starter),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	serveErr := make(chan error, 1)
	go func() {
		slog.Info("starting server", "addr", addr)
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
