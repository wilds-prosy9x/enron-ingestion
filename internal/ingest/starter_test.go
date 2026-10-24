package ingest_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/wilds-prosy9x/enron-ingestion/internal/ingest"
	"github.com/wilds-prosy9x/enron-ingestion/internal/source"
	"github.com/wilds-prosy9x/enron-ingestion/internal/store"
)

// fakeRunStore adds TryStart and RenewLease to fakeRecorder, and is its own
// store.Run. It allows one run at a time.
type fakeRunStore struct {
	*fakeRecorder
	mu       sync.Mutex
	running  bool
	modes    []string
	startErr error // ctx.Err() seen by TryStart
	tryErr   error // optional: returned by TryStart without starting a run
	renewErr error // optional: returned by RenewLease
	// renewFailures optionally limits renewErr to the first renewFailures renewals.
	renewFailures int
	renewals      int
	renewTimes    []time.Time
}

func (f *fakeRunStore) TryStart(ctx context.Context, dedupMode string) (store.Run, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.tryErr != nil {
		return nil, f.tryErr
	}
	if f.running {
		return nil, store.ErrAlreadyStarted
	}
	f.running = true
	f.modes = append(f.modes, dedupMode)
	f.startErr = ctx.Err()
	return f, nil
}

func (f *fakeRunStore) RenewLease(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.renewals++
	f.renewTimes = append(f.renewTimes, time.Now())
	if f.renewFailures > 0 && f.renewals > f.renewFailures {
		return nil
	}
	return f.renewErr
}

func (f *fakeRunStore) renewCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.renewals
}

func TestStarterRunsInBackground(t *testing.T) {
	st := &fakeRunStore{fakeRecorder: &fakeRecorder{}}
	src := &chanSource{ch: make(chan ingest.Message), mode: ingest.DedupMessageID}
	s := ingest.NewStarter(st, ingest.StarterConfig{Source: src, BatchSize: 1})
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
	// The run is recorded with the source's mode.
	if want := []string{"message_id"}; !reflect.DeepEqual(st.modes, want) {
		t.Errorf("TryStart modes = %v, want %v", st.modes, want)
	}

	src.send(t, msg(0))
	src.send(t, msg(1))
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
	s := ingest.NewStarter(st, ingest.StarterConfig{Source: src, BatchSize: 1})

	if err := s.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// Once the source has taken msg(1), it has handed msg(0) to the runner.
	src.send(t, msg(0))
	src.send(t, msg(1))

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
	if finished || failed != "ingestion cancelled" {
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

// TestStarterStartError checks that when TryStart fails, Start returns its
// error without starting a run, and a later Start still works.
func TestStarterStartError(t *testing.T) {
	tryErr := errors.New("dial tcp 10.0.0.5:6379: connection refused")
	st := &fakeRunStore{fakeRecorder: &fakeRecorder{}, tryErr: tryErr}
	src := &chanSource{ch: make(chan ingest.Message)}
	s := ingest.NewStarter(st, ingest.StarterConfig{Source: src, LeaseRenewInterval: time.Millisecond})
	t.Cleanup(s.Stop)

	if err := s.Start(t.Context()); !errors.Is(err, tryErr) {
		t.Fatalf("Start: err = %v, want %v", err, tryErr)
	}
	// A started run would renew its lease every millisecond.
	time.Sleep(20 * time.Millisecond)
	if n := st.renewCount(); n != 0 {
		t.Errorf("lease renewed %d times, want no run", n)
	}

	st.mu.Lock()
	st.tryErr = nil
	st.mu.Unlock()
	if err := s.Start(t.Context()); err != nil {
		t.Fatalf("Start after the error: %v", err)
	}
	src.send(t, msg(0))
	close(src.ch)
	waitFor(t, "run to finish", func() bool {
		_, _, finished, _ := st.snapshot()
		return finished
	})
	if _, got, _, failed := st.snapshot(); !reflect.DeepEqual(got, keys(0, 1)) || failed != "" {
		t.Errorf("keys = %v, failed = %q; want %v and not failed", got, failed, keys(0, 1))
	}
}

// TestStarterStartIgnoresRequestCancel checks that a cancelled request can't
// interrupt TryStart, which could leave a run recorded as running with nothing
// running it.
func TestStarterStartIgnoresRequestCancel(t *testing.T) {
	st := &fakeRunStore{fakeRecorder: &fakeRecorder{}}
	s := ingest.NewStarter(st, ingest.StarterConfig{Source: &chanSource{ch: make(chan ingest.Message)}})
	t.Cleanup(s.Stop)

	reqCtx, cancelReq := context.WithCancel(t.Context())
	cancelReq()
	if err := s.Start(reqCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if st.startErr != nil {
		t.Errorf("TryStart got a done context: %v", st.startErr)
	}
}

func TestStarterRenewsLease(t *testing.T) {
	st := &fakeRunStore{fakeRecorder: &fakeRecorder{}}
	src := &chanSource{ch: make(chan ingest.Message)}
	s := ingest.NewStarter(st, ingest.StarterConfig{Source: src, LeaseRenewInterval: time.Millisecond})
	t.Cleanup(s.Stop)

	if err := s.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	waitFor(t, "lease renewals", func() bool { return st.renewCount() >= 3 })

	// Renewals stop with the run.
	close(src.ch)
	waitFor(t, "run to finish", func() bool {
		_, _, finished, _ := st.snapshot()
		return finished
	})
	s.Stop()
	n := st.renewCount()
	time.Sleep(20 * time.Millisecond)
	if got := st.renewCount(); got != n {
		t.Errorf("lease renewed %d times after the run ended", got-n)
	}
}

// TestStarterStopsReplacedRun checks that a run is cancelled once renewing its
// lease shows that the lease has expired or another run has replaced it.
func TestStarterStopsReplacedRun(t *testing.T) {
	st := &fakeRunStore{fakeRecorder: &fakeRecorder{}, renewErr: store.ErrNotRunning}
	s := ingest.NewStarter(st, ingest.StarterConfig{
		Source:             &chanSource{ch: make(chan ingest.Message)}, // never ends by itself
		LeaseRenewInterval: time.Millisecond,
	})
	t.Cleanup(s.Stop)

	if err := s.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	waitFor(t, "run to be cancelled", func() bool {
		_, _, _, failed := st.snapshot()
		return failed == "ingestion cancelled"
	})
	if got := st.renewCount(); got != 1 {
		t.Errorf("RenewLease called %d times, want 1", got)
	}
}

// TestStarterFailReasons checks that a failed run records a reason that names
// the kind of failure without the error's details.
func TestStarterFailReasons(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"archive not found", fmt.Errorf("open enron.tar.gz in s3://secret-bucket: %w", source.ErrNotFound), "dataset archive not found"},
		{"archive unreadable", fmt.Errorf("%w: read tar: %w", ingest.ErrReadArchive, io.ErrUnexpectedEOF), "dataset archive could not be read"},
		{"other error", errors.New("record batch: dial tcp 10.0.0.5:6379: connection refused"), "internal error"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := &fakeRunStore{fakeRecorder: &fakeRecorder{}}
			src := &chanSource{ch: messages(0), err: tt.err}
			s := ingest.NewStarter(st, ingest.StarterConfig{Source: src})
			t.Cleanup(s.Stop)

			if err := s.Start(t.Context()); err != nil {
				t.Fatalf("Start: %v", err)
			}
			waitFor(t, "run to fail", func() bool {
				_, _, _, failed := st.snapshot()
				return failed != ""
			})
			if _, _, _, failed := st.snapshot(); failed != tt.want {
				t.Errorf("failed = %q, want %q", failed, tt.want)
			}
		})
	}
}

// TestStarterKeepsRenewingAfterErrors checks that a failed renewal other than
// ErrNotRunning doesn't stop the run.
func TestStarterKeepsRenewingAfterErrors(t *testing.T) {
	st := &fakeRunStore{fakeRecorder: &fakeRecorder{}, renewErr: errors.New("connection refused")}
	s := ingest.NewStarter(st, ingest.StarterConfig{
		Source:             &chanSource{ch: make(chan ingest.Message)},
		LeaseRenewInterval: time.Millisecond,
	})
	t.Cleanup(s.Stop)

	if err := s.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	waitFor(t, "lease renewals", func() bool { return st.renewCount() >= 3 })
	if _, _, finished, failed := st.snapshot(); finished || failed != "" {
		t.Errorf("finished = %v, failed = %q; want the run still in progress", finished, failed)
	}
}

// TestStarterRetriesFailedRenewalSoon checks that a failed renewal is retried
// after LeaseRetryInterval rather than a whole LeaseRenewInterval, so that a few
// failures in a row don't let the lease expire, and that renewals go back to
// LeaseRenewInterval once one succeeds.
func TestStarterRetriesFailedRenewalSoon(t *testing.T) {
	const interval = 200 * time.Millisecond
	st := &fakeRunStore{fakeRecorder: &fakeRecorder{}, renewErr: errors.New("i/o timeout"), renewFailures: 3}
	s := ingest.NewStarter(st, ingest.StarterConfig{
		Source:             &chanSource{ch: make(chan ingest.Message)},
		LeaseRenewInterval: interval,
		LeaseRetryInterval: time.Millisecond,
	})
	t.Cleanup(s.Stop)

	if err := s.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// 3 failures, the successful retry and the next regular renewal.
	waitFor(t, "lease renewals", func() bool { return st.renewCount() >= 5 })

	st.mu.Lock()
	times := st.renewTimes[:5]
	st.mu.Unlock()
	for i := 1; i < 4; i++ {
		if gap := times[i].Sub(times[i-1]); gap >= interval/2 {
			t.Errorf("retry %d came %v after the failure, want well under %v", i, gap, interval)
		}
	}
	if gap := times[4].Sub(times[3]); gap < interval {
		t.Errorf("renewal after a success came after %v, want at least %v", gap, interval)
	}
}
