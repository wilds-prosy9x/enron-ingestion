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

	"github.com/wilds-prosy9x/enron-ingestion/internal/server"
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

// run starts the HTTP server and blocks until ctx is cancelled or the server
// fails. stopSignals is called once shutdown begins so that a second signal
// terminates the process immediately instead of waiting for the shutdown.
func run(ctx context.Context, stopSignals func()) error {
	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":8080"
	}

	srv := &http.Server{
		Addr:              addr,
		Handler:           server.NewHandler(),
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
