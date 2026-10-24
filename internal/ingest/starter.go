package ingest

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
)

// ErrStopped is returned by Starter.Start after Stop has been called.
var ErrStopped = errors.New("starter stopped")

// RunStore is the store a Starter needs. *store.Redis implements it.
type RunStore interface {
	Recorder
	TryStart(ctx context.Context, dedupMode string) error
}

// StarterConfig configures the runs a Starter starts.
type StarterConfig struct {
	Source    Source
	DedupMode DedupMode
	// BatchSize and FlushInterval are passed to the Runner; zero means its default.
	BatchSize     int
	FlushInterval time.Duration
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
// finished, and ErrStopped after Stop. ctx is only used to start the run; the
// run itself continues until it ends or Stop is called.
func (s *Starter) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return ErrStopped
	}
	if err := s.store.TryStart(ctx, string(s.cfg.DedupMode)); err != nil {
		return err
	}

	r := &Runner{
		Source:        s.cfg.Source,
		Recorder:      s.store,
		BatchSize:     s.cfg.BatchSize,
		FlushInterval: s.cfg.FlushInterval,
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		start := time.Now()
		slog.Info("ingestion started", "dedup_mode", s.cfg.DedupMode)
		if err := r.Run(s.ctx); err != nil {
			slog.Error("ingestion failed", "error", err, "duration", time.Since(start))
			return
		}
		slog.Info("ingestion finished", "duration", time.Since(start))
	}()
	return nil
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
