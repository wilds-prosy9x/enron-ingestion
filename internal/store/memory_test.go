package store_test

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/wilds-prosy9x/enron-ingestion/internal/store"
)

func mustStart(t *testing.T, s store.Store, dedupMode string) store.Run {
	t.Helper()
	run, err := s.TryStart(t.Context(), dedupMode)
	if err != nil {
		t.Fatalf("TryStart: %v", err)
	}
	return run
}

func mustStatus(t *testing.T, s store.Store) store.Status {
	t.Helper()
	st, err := s.Status(t.Context())
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	return st
}

func mustTop(t *testing.T, s store.Store, n int) []store.SenderCount {
	t.Helper()
	top, err := s.TopSenders(t.Context(), n)
	if err != nil {
		t.Fatalf("TopSenders: %v", err)
	}
	return top
}

func mustRecord(t *testing.T, run store.Run, records ...store.Record) {
	t.Helper()
	if err := run.RecordBatch(t.Context(), records); err != nil {
		t.Fatalf("RecordBatch: %v", err)
	}
}

// checkEnded checks that all of run's methods report that it isn't running.
func checkEnded(t *testing.T, run store.Run) {
	t.Helper()
	ctx := t.Context()
	if err := run.RecordBatch(ctx, []store.Record{{Key: "late", From: "a@enron.com"}}); !errors.Is(err, store.ErrNotRunning) {
		t.Errorf("RecordBatch: err = %v, want ErrNotRunning", err)
	}
	if err := run.RenewLease(ctx); !errors.Is(err, store.ErrNotRunning) {
		t.Errorf("RenewLease: err = %v, want ErrNotRunning", err)
	}
	if err := run.Finish(ctx); !errors.Is(err, store.ErrNotRunning) {
		t.Errorf("Finish: err = %v, want ErrNotRunning", err)
	}
	if err := run.Fail(ctx, "late"); !errors.Is(err, store.ErrNotRunning) {
		t.Errorf("Fail: err = %v, want ErrNotRunning", err)
	}
}

func TestInitialState(t *testing.T) {
	s := store.NewMemory()

	if got, want := mustStatus(t, s), (store.Status{State: store.StateNotStarted}); got != want {
		t.Errorf("Status = %+v, want %+v", got, want)
	}
	if top := mustTop(t, s, 10); top == nil || len(top) != 0 {
		t.Errorf("TopSenders = %#v, want empty", top)
	}
}

func TestRecordBatch(t *testing.T) {
	s := store.NewMemory()
	run := mustStart(t, s, "content")

	mustRecord(t, run,
		store.Record{Key: "k1", From: "a@enron.com"},
		store.Record{Key: "k2", From: "a@enron.com"},
		store.Record{Key: "k1", From: "a@enron.com"}, // duplicate within the batch
		store.Record{Key: "k3", From: "b@enron.com"},
		store.Record{Key: "k4", From: ""},
	)
	mustRecord(t, run,
		store.Record{Key: "k3", From: "b@enron.com"}, // duplicate across batches
		store.Record{Key: "k4", From: ""},            // duplicate without sender
		store.Record{Key: "k5", From: "c@enron.com"},
		store.Record{Key: "k6", From: "b@enron.com"},
	)
	mustRecord(t, run) // empty batch is a no-op

	want := store.Status{State: store.StateRunning, DedupMode: "content", Processed: 9, Unique: 6, Skipped: 1}
	if got := mustStatus(t, s); got != want {
		t.Errorf("Status = %+v, want %+v", got, want)
	}
	// Equal counts are in alphabetical order.
	wantTop := []store.SenderCount{{"a@enron.com", 2}, {"b@enron.com", 2}, {"c@enron.com", 1}}
	if got := mustTop(t, s, 10); !reflect.DeepEqual(got, wantTop) {
		t.Errorf("TopSenders = %v, want %v", got, wantTop)
	}
}

func TestTopSendersLimit(t *testing.T) {
	s := store.NewMemory()
	run := mustStart(t, s, "content")
	// Sender i sends i emails.
	var records []store.Record
	for i := 1; i <= 12; i++ {
		for j := range i {
			records = append(records, store.Record{Key: fmt.Sprintf("s%d-%d", i, j), From: fmt.Sprintf("s%d@enron.com", i)})
		}
	}
	mustRecord(t, run, records...)

	top := mustTop(t, s, 10)
	if len(top) != 10 {
		t.Fatalf("got %d senders, want 10", len(top))
	}
	for i, sc := range top {
		want := store.SenderCount{Email: fmt.Sprintf("s%d@enron.com", 12-i), Count: int64(12 - i)}
		if sc != want {
			t.Errorf("top[%d] = %v, want %v", i, sc, want)
		}
	}
	if top := mustTop(t, s, 0); len(top) != 0 {
		t.Errorf("TopSenders(0) = %v, want empty", top)
	}
}

func TestLifecycle(t *testing.T) {
	s := store.NewMemory()
	ctx := t.Context()

	// First run fails part-way.
	first := mustStart(t, s, "message_id")
	if _, err := s.TryStart(ctx, "content"); !errors.Is(err, store.ErrAlreadyStarted) {
		t.Errorf("TryStart while running: err = %v, want ErrAlreadyStarted", err)
	}
	mustRecord(t, first, store.Record{Key: "k1", From: "a@enron.com"}, store.Record{Key: "k2", From: ""})
	if err := first.Fail(ctx, "archive truncated"); err != nil {
		t.Fatalf("Fail: %v", err)
	}
	want := store.Status{State: store.StateFailed, DedupMode: "message_id", Processed: 2, Unique: 2, Skipped: 1, Error: "archive truncated"}
	if got := mustStatus(t, s); got != want {
		t.Errorf("Status after Fail = %+v, want %+v", got, want)
	}
	checkEnded(t, first)

	// A failed run can be restarted; its data is discarded, so k1 counts again.
	second := mustStart(t, s, "content")
	want = store.Status{State: store.StateRunning, DedupMode: "content"}
	if got := mustStatus(t, s); got != want {
		t.Errorf("Status after restart = %+v, want %+v", got, want)
	}
	if top := mustTop(t, s, 10); len(top) != 0 {
		t.Errorf("TopSenders after restart = %v, want empty", top)
	}
	checkEnded(t, first) // the first run can't touch the second one
	mustRecord(t, second, store.Record{Key: "k1", From: "a@enron.com"})
	if err := second.Finish(ctx); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	want = store.Status{State: store.StateFinished, DedupMode: "content", Processed: 1, Unique: 1}
	if got := mustStatus(t, s); got != want {
		t.Errorf("Status after Finish = %+v, want %+v", got, want)
	}

	// A finished run can't be restarted or changed.
	if _, err := s.TryStart(ctx, "content"); !errors.Is(err, store.ErrAlreadyStarted) {
		t.Errorf("TryStart after Finish: err = %v, want ErrAlreadyStarted", err)
	}
	checkEnded(t, second)
	if got := mustStatus(t, s); got != want {
		t.Errorf("Status after late calls = %+v, want %+v", got, want)
	}
}

// TestConcurrentRecordBatch records batches from several goroutines while
// reading the status; run it with -race.
func TestConcurrentRecordBatch(t *testing.T) {
	s := store.NewMemory()
	run := mustStart(t, s, "content")

	const workers, batches = 8, 50
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			for b := range batches {
				// Every worker records the same keys, so each is counted once.
				rec := store.Record{Key: fmt.Sprintf("k%d", b), From: "a@enron.com"}
				if err := run.RecordBatch(t.Context(), []store.Record{rec}); err != nil {
					t.Errorf("RecordBatch: %v", err)
				}
				if w == 0 {
					s.Status(t.Context())
					s.TopSenders(t.Context(), 10)
				}
			}
		})
	}
	wg.Wait()

	want := store.Status{State: store.StateRunning, DedupMode: "content", Processed: workers * batches, Unique: batches}
	if got := mustStatus(t, s); got != want {
		t.Errorf("Status = %+v, want %+v", got, want)
	}
	if got, want := mustTop(t, s, 10), []store.SenderCount{{"a@enron.com", batches}}; !reflect.DeepEqual(got, want) {
		t.Errorf("TopSenders = %v, want %v", got, want)
	}
}
