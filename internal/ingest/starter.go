package ingest

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/wilds-prosy9x/enron-ingestion/internal/source"
	"github.com/wilds-prosy9x/enron-ingestion/internal/store"
)

// startTimeout limits how long recording a new run as started may take.
const startTimeout = 5 * time.Second

// ErrStopped is returned by Starter.Start after Stop has been called.
var ErrStopped = errors.New("starter stopped")

// RunStore is the store a Starter needs. *store.Redis implements it.
type RunStore interface {
	TryStart(ctx context.Context, dedupMode string) (store.Run, error)
}

// StarterConfig configures the runs a Starter starts.
type StarterConfig struct {
	Source Source
	// BatchSize and FlushInterval are passed to the Runner; zero means its default.
	BatchSize     int
	FlushInterval time.Duration
	// LeaseRenewInterval is how often a run's lease is renewed. Default store.LeaseTTL / 3.
	LeaseRenewInterval time.Duration
}

// Starter starts ingestion runs in the background.
type Starter struct {
	store RunStore
	cfg   StarterConfig

	// ctx is cancelled by Stop and cancels running runs.
	ctx    context.Context
	cancel context.CancelFunc

	mu      sync.Mutex // guards stopped and wg.Add
	stopped bool
	wg      sync.WaitGroup
}

// NewStarter returns a Starter that records runs in st.
func NewStarter(st RunStore, cfg StarterConfig) *Starter {
	ctx, cancel := context.WithCancel(context.Background())
	return &Starter{store: st, cfg: cfg, ctx: ctx, cancel: cancel}
}

// Start begins a run in the background and returns once it has been recorded
// as running. It returns store.ErrAlreadyStarted if a run is in progress or has
// finished, and ErrStopped after Stop. Cancelling ctx doesn't stop the run, nor
// interrupt starting it; the run continues until it ends or Stop is called.
func (s *Starter) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return ErrStopped
	}
	// If ctx were cancelled while TryStart waits for Redis's reply, the run
	// could be recorded as running with nothing running it.
	startCtx, cancelStart := context.WithTimeout(context.WithoutCancel(ctx), startTimeout)
	defer cancelStart()
	mode := s.cfg.Source.DedupMode()
	run, err := s.store.TryStart(startCtx, string(mode))
	if err != nil {
		return err
	}

	r := &Runner{
		Source:        s.cfg.Source,
		Recorder:      run,
		BatchSize:     s.cfg.BatchSize,
		FlushInterval: s.cfg.FlushInterval,
		FailReason:    failReason,
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		runCtx, stopRun := context.WithCancel(s.ctx)
		defer stopRun()
		leaseDone := make(chan struct{})
		go func() {
			defer close(leaseDone)
			s.keepLease(runCtx, run, stopRun)
		}()

		start := time.Now()
		slog.Info("ingestion started", "dedup_mode", mode)
		err := r.Run(runCtx)
		stopRun()
		<-leaseDone
		if err != nil {
			slog.Error("ingestion failed", "error", err, "duration", time.Since(start))
			return
		}
		slog.Info("ingestion finished", "duration", time.Since(start))
	}()
	return nil
}

// failReason returns the reason recorded for a run that failed with err. API
// clients see it, so it only names the kind of failure, leaving out details
// such as storage addresses and bucket URLs; the full error is logged.
func failReason(err error) string {
	switch {
	case errors.Is(err, context.Canceled):
		return "ingestion cancelled"
	case errors.Is(err, source.ErrNotFound):
		return "dataset archive not found"
	case errors.Is(err, ErrReadArchive):
		return "dataset archive could not be read"
	default:
		return "internal error"
	}
}

// keepLease renews run's lease until ctx is done. If the lease has expired or
// the run has been replaced by another one, it calls stopRun, as the run can't
// record anything more. Other errors are retried; the lease only expires if
// they last for store.LeaseTTL.
func (s *Starter) keepLease(ctx context.Context, run store.Run, stopRun func()) {
	interval := s.cfg.LeaseRenewInterval
	if interval <= 0 {
		interval = store.LeaseTTL / 3
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		err := run.RenewLease(ctx)
		switch {
		case err == nil, ctx.Err() != nil:
		case errors.Is(err, store.ErrNotRunning):
			slog.Error("ingestion run's lease has expired or it was replaced, stopping it")
			stopRun()
			return
		default:
			slog.Warn("renew ingestion lease", "error", err)
		}
	}
}

// Stop cancels any running run, which is then marked failed, and waits for it
// to end. Later calls to Start return ErrStopped.
func (s *Starter) Stop() {
	s.mu.Lock()
	s.stopped = true
	s.mu.Unlock()
	s.cancel()
	s.wg.Wait()
}
