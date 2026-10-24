package ingest_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wilds-prosy9x/enron-ingestion/internal/ingest"
	"github.com/wilds-prosy9x/enron-ingestion/internal/source"
	"github.com/wilds-prosy9x/enron-ingestion/internal/store"
)

// fakeRecorder keeps everything it is given in memory.
type fakeRecorder struct {
	mu        sync.Mutex
	batches   [][]store.Record
	finished  bool
	failed    string
	recordErr func(batch int) error // optional: error for the n-th batch (0-based)
	finishErr error
	// ctxErrs collects ctx.Err() seen by Finish and Fail, which should be nil.
	ctxErrs []error
}

func (f *fakeRecorder) RecordBatch(ctx context.Context, records []store.Record) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.recordErr != nil {
		if err := f.recordErr(len(f.batches)); err != nil {
			return err
		}
	}
	f.batches = append(f.batches, append([]store.Record(nil), records...))
	return nil
}

func (f *fakeRecorder) Finish(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ctxErrs = append(f.ctxErrs, ctx.Err())
	if f.finishErr != nil {
		return f.finishErr
	}
	f.finished = true
	return nil
}

func (f *fakeRecorder) Fail(ctx context.Context, reason string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ctxErrs = append(f.ctxErrs, ctx.Err())
	f.failed = reason
	return nil
}

// snapshot returns the batch sizes, all recorded keys, and the end state.
func (f *fakeRecorder) snapshot() (sizes []int, keys []string, finished bool, failed string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, b := range f.batches {
		sizes = append(sizes, len(b))
		for _, r := range b {
			keys = append(keys, r.Key)
		}
	}
	return sizes, keys, f.finished, f.failed
}

// chanSource yields messages sent on ch until it is closed, then returns err.
type chanSource struct {
	ch  chan ingest.Message
	err error
}

func (s *chanSource) Messages(ctx context.Context, fn func(ingest.Message) error) error {
	for {
		select {
		case m, ok := <-s.ch:
			if !ok {
				return s.err
			}
			if err := fn(m); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// messages returns a closed channel holding msgs k0..k(n-1).
func messages(n int) chan ingest.Message {
	ch := make(chan ingest.Message, n)
	for i := range n {
		ch <- msg(i)
	}
	close(ch)
	return ch
}

func msg(i int) ingest.Message {
	return ingest.Message{Key: fmt.Sprintf("k%d", i), From: "a@enron.com"}
}

func keys(from, to int) []string {
	var out []string
	for i := from; i < to; i++ {
		out = append(out, fmt.Sprintf("k%d", i))
	}
	return out
}

// runAsync runs r in the background and returns a function that waits for its result.
func runAsync(t *testing.T, ctx context.Context, r *ingest.Runner) func() error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	return func() error {
		t.Helper()
		select {
		case err := <-done:
			return err
		case <-time.After(5 * time.Second):
			t.Fatal("Run did not return")
			return nil
		}
	}
}

// waitFor polls cond until it is true or the test times out.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func checkEndCtx(t *testing.T, rec *fakeRecorder) {
	t.Helper()
	for _, err := range rec.ctxErrs {
		if err != nil {
			t.Errorf("Finish/Fail called with a done context: %v", err)
		}
	}
}

func TestRunnerRecordsAllInBatches(t *testing.T) {
	rec := &fakeRecorder{}
	r := &ingest.Runner{Source: &chanSource{ch: messages(7)}, Recorder: rec, BatchSize: 3, FlushInterval: time.Hour}

	if err := r.Run(t.Context()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	sizes, got, finished, failed := rec.snapshot()
	if want := []int{3, 3, 1}; !reflect.DeepEqual(sizes, want) {
		t.Errorf("batch sizes = %v, want %v", sizes, want)
	}
	if want := keys(0, 7); !reflect.DeepEqual(got, want) {
		t.Errorf("keys = %v, want %v", got, want)
	}
	if !finished || failed != "" {
		t.Errorf("finished = %v, failed = %q; want finished", finished, failed)
	}
	checkEndCtx(t, rec)
}

func TestRunnerFlushesPartialBatchAfterInterval(t *testing.T) {
	rec := &fakeRecorder{}
	src := &chanSource{ch: make(chan ingest.Message)}
	r := &ingest.Runner{Source: src, Recorder: rec, BatchSize: 100, FlushInterval: 20 * time.Millisecond}
	wait := runAsync(t, t.Context(), r)

	src.ch <- msg(0)
	src.ch <- msg(1)
	waitFor(t, "partial batch to be recorded", func() bool {
		_, got, _, _ := rec.snapshot()
		return len(got) == 2
	})
	if _, _, finished, _ := rec.snapshot(); finished {
		t.Error("finished before the source ended")
	}

	src.ch <- msg(2)
	close(src.ch)
	if err := wait(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	_, got, finished, _ := rec.snapshot()
	if want := keys(0, 3); !reflect.DeepEqual(got, want) {
		t.Errorf("keys = %v, want %v", got, want)
	}
	if !finished {
		t.Error("not finished")
	}
}

func TestRunnerSourceError(t *testing.T) {
	rec := &fakeRecorder{}
	readErr := errors.New("archive truncated")
	r := &ingest.Runner{Source: &chanSource{ch: messages(2), err: readErr}, Recorder: rec, BatchSize: 10, FlushInterval: time.Hour}

	err := r.Run(t.Context())
	if !errors.Is(err, readErr) {
		t.Fatalf("Run: err = %v, want %v", err, readErr)
	}

	_, got, finished, failed := rec.snapshot()
	// Messages read before the error are still recorded.
	if want := keys(0, 2); !reflect.DeepEqual(got, want) {
		t.Errorf("keys = %v, want %v", got, want)
	}
	if finished || !strings.Contains(failed, "archive truncated") {
		t.Errorf("finished = %v, failed = %q; want failed with the read error", finished, failed)
	}
	checkEndCtx(t, rec)
}

func TestRunnerRecordError(t *testing.T) {
	recordErr := errors.New("redis down")
	rec := &fakeRecorder{recordErr: func(batch int) error {
		if batch == 1 {
			return recordErr
		}
		return nil
	}}
	// The source has more messages than get read, so the reader must be stopped.
	src := &chanSource{ch: make(chan ingest.Message)}
	go func() {
		for i := 0; ; i++ {
			select {
			case src.ch <- msg(i):
			case <-t.Context().Done():
				return
			}
		}
	}()
	r := &ingest.Runner{Source: src, Recorder: rec, BatchSize: 2, FlushInterval: time.Hour}

	err := runAsync(t, t.Context(), r)()
	if !errors.Is(err, recordErr) {
		t.Fatalf("Run: err = %v, want %v", err, recordErr)
	}
	_, got, finished, failed := rec.snapshot()
	if want := keys(0, 2); !reflect.DeepEqual(got, want) {
		t.Errorf("keys = %v, want %v", got, want)
	}
	if finished || !strings.Contains(failed, "redis down") {
		t.Errorf("finished = %v, failed = %q; want failed with the record error", finished, failed)
	}
}

func TestRunnerRunEndedElsewhere(t *testing.T) {
	rec := &fakeRecorder{recordErr: func(int) error { return store.ErrNotRunning }}
	r := &ingest.Runner{Source: &chanSource{ch: messages(3)}, Recorder: rec, BatchSize: 1}

	err := r.Run(t.Context())
	if !errors.Is(err, store.ErrNotRunning) {
		t.Fatalf("Run: err = %v, want ErrNotRunning", err)
	}
	// The run already ended, so the runner must not try to mark it.
	if _, _, finished, failed := rec.snapshot(); finished || failed != "" {
		t.Errorf("finished = %v, failed = %q; want neither", finished, failed)
	}
}

func TestRunnerCancelled(t *testing.T) {
	rec := &fakeRecorder{}
	src := &chanSource{ch: make(chan ingest.Message)}
	r := &ingest.Runner{Source: src, Recorder: rec, BatchSize: 100, FlushInterval: time.Hour}
	ctx, cancel := context.WithCancel(t.Context())
	wait := runAsync(t, ctx, r)

	// Once the source has taken msg(2), it has handed msg(0) and msg(1) to the runner.
	// Whether msg(2) gets through before the cancellation is down to timing.
	src.ch <- msg(0)
	src.ch <- msg(1)
	src.ch <- msg(2)
	cancel()

	err := wait()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run: err = %v, want context.Canceled", err)
	}
	_, got, finished, failed := rec.snapshot()
	// Messages read before the cancellation are recorded.
	if !reflect.DeepEqual(got, keys(0, 2)) && !reflect.DeepEqual(got, keys(0, 3)) {
		t.Errorf("keys = %v, want %v or %v", got, keys(0, 2), keys(0, 3))
	}
	if finished || !strings.Contains(failed, "context canceled") {
		t.Errorf("finished = %v, failed = %q; want failed with the cancellation", finished, failed)
	}
	checkEndCtx(t, rec)
}

func TestRunnerFinishError(t *testing.T) {
	finishErr := errors.New("redis down")
	rec := &fakeRecorder{finishErr: finishErr}
	r := &ingest.Runner{Source: &chanSource{ch: messages(1)}, Recorder: rec}

	if err := r.Run(t.Context()); !errors.Is(err, finishErr) {
		t.Errorf("Run: err = %v, want %v", err, finishErr)
	}
}

func TestArchiveSource(t *testing.T) {
	dir := t.TempDir()
	archive := buildArchive(t, []file{
		{"maildir/a/inbox/1.", email("Hi", "Message-ID: <1@x>", "From: a@enron.com")},
		{"maildir/a/inbox/2.", email("Hi", "Message-ID: <2@x>", "From: b@enron.com")},
	})
	if err := os.WriteFile(filepath.Join(dir, "enron.tar.gz"), archive, 0o644); err != nil {
		t.Fatalf("write archive: %v", err)
	}

	t.Run("reads all messages", func(t *testing.T) {
		src := ingest.ArchiveSource{BucketURL: "file://" + dir, Key: "enron.tar.gz", Mode: ingest.DedupContent}
		var from []string
		err := src.Messages(t.Context(), func(m ingest.Message) error {
			from = append(from, m.From)
			return nil
		})
		if err != nil {
			t.Fatalf("Messages: %v", err)
		}
		if want := []string{"a@enron.com", "b@enron.com"}; !reflect.DeepEqual(from, want) {
			t.Errorf("senders = %v, want %v", from, want)
		}
	})

	t.Run("missing archive", func(t *testing.T) {
		src := ingest.ArchiveSource{BucketURL: "file://" + dir, Key: "missing.tar.gz", Mode: ingest.DedupContent}
		err := src.Messages(t.Context(), func(ingest.Message) error { return nil })
		if !errors.Is(err, source.ErrNotFound) {
			t.Errorf("err = %v, want ErrNotFound", err)
		}
	})

	t.Run("stops when cancelled", func(t *testing.T) {
		src := ingest.ArchiveSource{BucketURL: "file://" + dir, Key: "enron.tar.gz", Mode: ingest.DedupContent}
		ctx, cancel := context.WithCancel(t.Context())
		calls := 0
		err := src.Messages(ctx, func(ingest.Message) error {
			calls++
			cancel()
			return nil
		})
		if !errors.Is(err, context.Canceled) {
			t.Errorf("err = %v, want context.Canceled", err)
		}
		if calls != 1 {
			t.Errorf("fn called %d times, want 1", calls)
		}
	})
}
