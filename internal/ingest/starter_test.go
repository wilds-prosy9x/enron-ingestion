package ingest_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wilds-prosy9x/enron-ingestion/internal/ingest"
	"github.com/wilds-prosy9x/enron-ingestion/internal/store"
)

// fakeRunStore adds TryStart to fakeRecorder. It allows one run at a time.
type fakeRunStore struct {
	*fakeRecorder
	mu      sync.Mutex
	running bool
	modes   []string
}

func (f *fakeRunStore) TryStart(ctx context.Context, dedupMode string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.running {
		return store.ErrAlreadyStarted
	}
	f.running = true
	f.modes = append(f.modes, dedupMode)
	return nil
}

func TestStarterRunsInBackground(t *testing.T) {
	st := &fakeRunStore{fakeRecorder: &fakeRecorder{}}
	src := &chanSource{ch: make(chan ingest.Message)}
	s := ingest.NewStarter(st, ingest.StarterConfig{Source: src, DedupMode: ingest.DedupContent, BatchSize: 1})
	t.Cleanup(s.Stop)

	// The request context ending must not stop the run.
	reqCtx, cancelReq := context.WithCancel(t.Context())
	if err := s.Start(reqCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	cancelReq()

	if err := s.Start(t.Context()); !errors.Is(err, store.ErrAlreadyStarted) {
		t.Errorf("second Start: err = %v, want ErrAlreadyStarted", err)
	}
	if want := []string{"content"}; !reflect.DeepEqual(st.modes, want) {
		t.Errorf("TryStart modes = %v, want %v", st.modes, want)
	}

	src.ch <- msg(0)
	src.ch <- msg(1)
	close(src.ch)
	waitFor(t, "run to finish", func() bool {
		_, _, finished, _ := st.snapshot()
		return finished
	})
	if _, got, _, failed := st.snapshot(); !reflect.DeepEqual(got, keys(0, 2)) || failed != "" {
		t.Errorf("keys = %v, failed = %q; want %v and not failed", got, failed, keys(0, 2))
	}
}

func TestStarterStopCancelsRun(t *testing.T) {
	st := &fakeRunStore{fakeRecorder: &fakeRecorder{}}
	src := &chanSource{ch: make(chan ingest.Message)}
	s := ingest.NewStarter(st, ingest.StarterConfig{Source: src, DedupMode: ingest.DedupContent, BatchSize: 1})

	if err := s.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// Once the source has taken msg(1), it has handed msg(0) to the runner.
	src.ch <- msg(0)
	src.ch <- msg(1)

	stopped := make(chan struct{})
	go func() {
		s.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return")
	}

	// Stop waits for the run to end, so the result is visible right away.
	_, got, finished, failed := st.snapshot()
	if !reflect.DeepEqual(got, keys(0, 1)) && !reflect.DeepEqual(got, keys(0, 2)) {
		t.Errorf("keys = %v, want %v or %v", got, keys(0, 1), keys(0, 2))
	}
	if finished || !strings.Contains(failed, "context canceled") {
		t.Errorf("finished = %v, failed = %q; want failed with the cancellation", finished, failed)
	}

	if err := s.Start(t.Context()); !errors.Is(err, ingest.ErrStopped) {
		t.Errorf("Start after Stop: err = %v, want ErrStopped", err)
	}
	s.Stop() // a second Stop is harmless
}

func TestStarterStopWithoutRun(t *testing.T) {
	s := ingest.NewStarter(&fakeRunStore{fakeRecorder: &fakeRecorder{}}, ingest.StarterConfig{})
	s.Stop()
}
