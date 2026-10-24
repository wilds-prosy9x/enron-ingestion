package store_test

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/wilds-prosy9x/enron-ingestion/internal/redistest"
	"github.com/wilds-prosy9x/enron-ingestion/internal/store"
)

func TestMain(m *testing.M) {
	os.Exit(redistest.Run(m))
}

// newStore returns a store with keys unique to the test.
func newStore(t *testing.T) *store.Redis {
	t.Helper()
	return store.NewRedis(redistest.Client(t), redistest.Prefix(t))
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

func mustRecord(t *testing.T, s store.Store, records ...store.Record) {
	t.Helper()
	if err := s.RecordBatch(t.Context(), records); err != nil {
		t.Fatalf("RecordBatch: %v", err)
	}
}

func TestInitialState(t *testing.T) {
	s := newStore(t)

	if got, want := mustStatus(t, s), (store.Status{State: store.StateNotStarted}); got != want {
		t.Errorf("Status = %+v, want %+v", got, want)
	}
	if top := mustTop(t, s, 10); len(top) != 0 {
		t.Errorf("TopSenders = %v, want empty", top)
	}
}

func TestRecordBatch(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()
	if err := s.TryStart(ctx, "content"); err != nil {
		t.Fatalf("TryStart: %v", err)
	}

	mustRecord(t, s,
		store.Record{Key: "k1", From: "a@enron.com"},
		store.Record{Key: "k2", From: "a@enron.com"},
		store.Record{Key: "k1", From: "a@enron.com"}, // duplicate within the batch
		store.Record{Key: "k3", From: "b@enron.com"},
		store.Record{Key: "k4", From: ""},
	)
	mustRecord(t, s,
		store.Record{Key: "k3", From: "b@enron.com"}, // duplicate across batches
		store.Record{Key: "k4", From: ""},            // duplicate without sender
		store.Record{Key: "k5", From: "c@enron.com"},
		store.Record{Key: "k6", From: "b@enron.com"},
	)
	mustRecord(t, s) // empty batch is a no-op

	want := store.Status{State: store.StateRunning, DedupMode: "content", Processed: 9, Unique: 6, Skipped: 1}
	if got := mustStatus(t, s); got != want {
		t.Errorf("Status = %+v, want %+v", got, want)
	}
	wantTop := []store.SenderCount{{"b@enron.com", 2}, {"a@enron.com", 2}, {"c@enron.com", 1}}
	if got := mustTop(t, s, 10); !reflect.DeepEqual(got, wantTop) {
		t.Errorf("TopSenders = %v, want %v", got, wantTop)
	}
}

func TestTopSendersLimit(t *testing.T) {
	s := newStore(t)
	if err := s.TryStart(t.Context(), "content"); err != nil {
		t.Fatalf("TryStart: %v", err)
	}
	// Sender i sends i emails.
	var records []store.Record
	for i := 1; i <= 12; i++ {
		for j := range i {
			records = append(records, store.Record{Key: fmt.Sprintf("s%d-%d", i, j), From: fmt.Sprintf("s%d@enron.com", i)})
		}
	}
	mustRecord(t, s, records...)

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
	s := newStore(t)
	ctx := t.Context()

	// Nothing to record, finish or fail before a run starts.
	if err := s.RecordBatch(ctx, []store.Record{{Key: "k", From: "a@enron.com"}}); !errors.Is(err, store.ErrNotRunning) {
		t.Errorf("RecordBatch before start: err = %v, want ErrNotRunning", err)
	}
	if err := s.Finish(ctx); !errors.Is(err, store.ErrNotRunning) {
		t.Errorf("Finish before start: err = %v, want ErrNotRunning", err)
	}
	if err := s.Fail(ctx, "boom"); !errors.Is(err, store.ErrNotRunning) {
		t.Errorf("Fail before start: err = %v, want ErrNotRunning", err)
	}

	// First run fails part-way.
	if err := s.TryStart(ctx, "message_id"); err != nil {
		t.Fatalf("TryStart: %v", err)
	}
	if err := s.TryStart(ctx, "content"); !errors.Is(err, store.ErrAlreadyStarted) {
		t.Errorf("TryStart while running: err = %v, want ErrAlreadyStarted", err)
	}
	mustRecord(t, s, store.Record{Key: "k1", From: "a@enron.com"}, store.Record{Key: "k2", From: ""})
	if err := s.Fail(ctx, "archive truncated"); err != nil {
		t.Fatalf("Fail: %v", err)
	}
	want := store.Status{State: store.StateFailed, DedupMode: "message_id", Processed: 2, Unique: 2, Skipped: 1, Error: "archive truncated"}
	if got := mustStatus(t, s); got != want {
		t.Errorf("Status after Fail = %+v, want %+v", got, want)
	}
	if err := s.RecordBatch(ctx, []store.Record{{Key: "k3", From: "a@enron.com"}}); !errors.Is(err, store.ErrNotRunning) {
		t.Errorf("RecordBatch after Fail: err = %v, want ErrNotRunning", err)
	}

	// A failed run can be restarted; its data is discarded, so k1 counts again.
	if err := s.TryStart(ctx, "content"); err != nil {
		t.Fatalf("TryStart after Fail: %v", err)
	}
	want = store.Status{State: store.StateRunning, DedupMode: "content"}
	if got := mustStatus(t, s); got != want {
		t.Errorf("Status after restart = %+v, want %+v", got, want)
	}
	if top := mustTop(t, s, 10); len(top) != 0 {
		t.Errorf("TopSenders after restart = %v, want empty", top)
	}
	mustRecord(t, s, store.Record{Key: "k1", From: "a@enron.com"})
	if err := s.Finish(ctx); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	want = store.Status{State: store.StateFinished, DedupMode: "content", Processed: 1, Unique: 1}
	if got := mustStatus(t, s); got != want {
		t.Errorf("Status after Finish = %+v, want %+v", got, want)
	}

	// A finished run can't be restarted or changed.
	if err := s.TryStart(ctx, "content"); !errors.Is(err, store.ErrAlreadyStarted) {
		t.Errorf("TryStart after Finish: err = %v, want ErrAlreadyStarted", err)
	}
	if err := s.Fail(ctx, "late"); !errors.Is(err, store.ErrNotRunning) {
		t.Errorf("Fail after Finish: err = %v, want ErrNotRunning", err)
	}
	if got := mustStatus(t, s).State; got != store.StateFinished {
		t.Errorf("State = %s, want finished", got)
	}
}

func TestStoresAreIsolatedByPrefix(t *testing.T) {
	client := redistest.Client(t)
	a := store.NewRedis(client, redistest.Prefix(t))
	b := store.NewRedis(client, redistest.Prefix(t))
	if err := a.TryStart(t.Context(), "content"); err != nil {
		t.Fatalf("TryStart: %v", err)
	}
	if got := mustStatus(t, b).State; got != store.StateNotStarted {
		t.Errorf("other store's State = %s, want not_started", got)
	}
}
