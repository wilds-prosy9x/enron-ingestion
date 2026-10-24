package store_test

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

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

// leaseKey returns the key holding the lease of the stores with prefix.
func leaseKey(prefix string) string {
	return "{" + prefix + "}:lease"
}

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
	wantTop := []store.SenderCount{{"b@enron.com", 2}, {"a@enron.com", 2}, {"c@enron.com", 1}}
	if got := mustTop(t, s, 10); !reflect.DeepEqual(got, wantTop) {
		t.Errorf("TopSenders = %v, want %v", got, wantTop)
	}
}

func TestTopSendersLimit(t *testing.T) {
	s := newStore(t)
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
	s := newStore(t)
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

func TestLease(t *testing.T) {
	client := redistest.Client(t)
	prefix := redistest.Prefix(t)
	s := store.NewRedis(client, prefix)
	ctx := t.Context()

	checkTTL := func(when string) {
		t.Helper()
		ttl, err := client.PTTL(ctx, leaseKey(prefix)).Result()
		if err != nil {
			t.Fatalf("PTTL: %v", err)
		}
		if ttl <= 0 || ttl > store.LeaseTTL {
			t.Errorf("lease TTL %s = %v, want in (0, %v]", when, ttl, store.LeaseTTL)
		}
	}

	run := mustStart(t, s, "content")
	checkTTL("after start")

	client.PExpire(ctx, leaseKey(prefix), store.LeaseTTL/2)
	if err := run.RenewLease(ctx); err != nil {
		t.Fatalf("RenewLease: %v", err)
	}
	if ttl := client.PTTL(ctx, leaseKey(prefix)).Val(); ttl <= store.LeaseTTL/2 {
		t.Errorf("lease TTL after renewal = %v, want more than %v", ttl, store.LeaseTTL/2)
	}
	checkTTL("after renewal")
	if _, err := s.TryStart(ctx, "content"); !errors.Is(err, store.ErrAlreadyStarted) {
		t.Errorf("TryStart with a renewed lease: err = %v, want ErrAlreadyStarted", err)
	}

	// Ending the run drops the lease.
	if err := run.Fail(ctx, "boom"); err != nil {
		t.Fatalf("Fail: %v", err)
	}
	if client.Exists(ctx, leaseKey(prefix)).Val() != 0 {
		t.Errorf("lease exists after Fail")
	}
}

// TestExpiredLease checks that a run whose lease has expired, e.g. because its
// instance lost its connection to Redis, is reported as failed, and stays so
// even if the instance comes back.
func TestExpiredLease(t *testing.T) {
	client := redistest.Client(t)
	prefix := redistest.Prefix(t)
	s := store.NewRedis(client, prefix)

	run := mustStart(t, s, "content")
	mustRecord(t, run, store.Record{Key: "k1", From: "a@enron.com"})
	// Let the lease actually expire.
	client.PExpire(t.Context(), leaseKey(prefix), time.Millisecond)
	time.Sleep(10 * time.Millisecond)

	want := store.Status{State: store.StateFailed, DedupMode: "content", Processed: 1, Unique: 1, Error: store.ReasonInterrupted}
	if got := mustStatus(t, s); got != want {
		t.Errorf("Status = %+v, want %+v", got, want)
	}
	checkEnded(t, run)
	if got := mustStatus(t, s); got != want {
		t.Errorf("Status after late calls = %+v, want %+v", got, want)
	}
}

// TestAbandonedRunIsReplaced checks that once a run's lease has expired,
// e.g. because its process died, another instance can start a new run, and
// the old run can no longer change anything.
func TestAbandonedRunIsReplaced(t *testing.T) {
	client := redistest.Client(t)
	prefix := redistest.Prefix(t)
	// Two stores with the same prefix act as two app instances.
	a := store.NewRedis(client, prefix)
	b := store.NewRedis(client, prefix)
	ctx := t.Context()

	old := mustStart(t, a, "message_id")
	mustRecord(t, old, store.Record{Key: "k1", From: "a@enron.com"})
	if _, err := b.TryStart(ctx, "content"); !errors.Is(err, store.ErrAlreadyStarted) {
		t.Errorf("TryStart with a live lease: err = %v, want ErrAlreadyStarted", err)
	}

	client.Del(ctx, leaseKey(prefix)) // as if the lease expired
	if got := mustStatus(t, b); got.State != store.StateFailed || got.Error != store.ReasonInterrupted {
		t.Errorf("Status of the abandoned run = %+v, want failed with %q", got, store.ReasonInterrupted)
	}
	replacement := mustStart(t, b, "content")
	checkEnded(t, old)
	mustRecord(t, replacement, store.Record{Key: "k2", From: "b@enron.com"})

	want := store.Status{State: store.StateRunning, DedupMode: "content", Processed: 1, Unique: 1}
	if got := mustStatus(t, a); got != want {
		t.Errorf("Status = %+v, want %+v", got, want)
	}
	if got, want := mustTop(t, a, 10), []store.SenderCount{{"b@enron.com", 1}}; !reflect.DeepEqual(got, want) {
		t.Errorf("TopSenders = %v, want %v", got, want)
	}
}

// TestStartDeletesOldOwner checks that starting a run deletes the owner key of
// earlier versions, which they check before changing anything, so that an
// instance running one stops once its run has been replaced.
func TestStartDeletesOldOwner(t *testing.T) {
	client := redistest.Client(t)
	prefix := redistest.Prefix(t)
	s := store.NewRedis(client, prefix)
	ctx := t.Context()
	ownerKey := "{" + prefix + "}:owner"

	// As left by an earlier version whose run's lease has expired.
	client.Set(ctx, "{"+prefix+"}:state", string(store.StateRunning), 0)
	client.Set(ctx, ownerKey, "old-token", 0)

	mustStart(t, s, "content")
	if client.Exists(ctx, ownerKey).Val() != 0 {
		t.Errorf("owner key exists after TryStart")
	}
}

func TestStoresAreIsolatedByPrefix(t *testing.T) {
	client := redistest.Client(t)
	a := store.NewRedis(client, redistest.Prefix(t))
	b := store.NewRedis(client, redistest.Prefix(t))
	mustStart(t, a, "content")
	if got := mustStatus(t, b).State; got != store.StateNotStarted {
		t.Errorf("other store's State = %s, want not_started", got)
	}
}
