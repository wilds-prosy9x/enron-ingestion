// Package store keeps the ingestion state and sender counts.
package store

import (
	"context"
	"errors"
)

// State is the lifecycle state of an ingestion run.
type State string

const (
	StateNotStarted State = "not_started"
	StateRunning    State = "running"
	StateFinished   State = "finished"
	StateFailed     State = "failed"
)

var (
	// ErrAlreadyStarted is returned by TryStart when a run is in progress or has finished.
	ErrAlreadyStarted = errors.New("ingestion already started")
	// ErrNotRunning is returned by a Run's methods once the run has ended.
	ErrNotRunning = errors.New("ingestion not running")
)

// Record is one email to count.
type Record struct {
	// Key identifies the email: records with the same Key are counted once.
	Key string
	// From is the sender address; empty if the email has no usable sender.
	From string
}

// Status is a snapshot of the ingestion progress.
type Status struct {
	State State
	// DedupMode is the mode passed to TryStart; empty before the first run.
	DedupMode string
	// Processed is the number of records received, including duplicates.
	Processed int64
	// Unique is the number of distinct keys.
	Unique int64
	// Skipped is the number of distinct keys without a usable sender.
	Skipped int64
	// Error is the reason passed to Fail; empty unless State is StateFailed.
	Error string
}

// SenderCount is a sender and the number of distinct emails they sent.
type SenderCount struct {
	Email string
	Count int64
}

// Store keeps the ingestion state and counts. Implementations must be safe for
// concurrent use.
type Store interface {
	// TryStart begins a new run, discarding the data of any previous failed run,
	// and returns it. It returns ErrAlreadyStarted if a run is in progress or has
	// finished.
	TryStart(ctx context.Context, dedupMode string) (Run, error)
	// Status returns the current progress.
	Status(ctx context.Context) (Status, error)
	// TopSenders returns up to n senders with the most emails, highest count first.
	TopSenders(ctx context.Context, n int) ([]SenderCount, error)
}

// Run is a run started by TryStart. Its methods return ErrNotRunning once the
// run has ended.
type Run interface {
	// RecordBatch counts the records.
	RecordBatch(ctx context.Context, records []Record) error
	// Finish marks the run as finished.
	Finish(ctx context.Context) error
	// Fail marks the run as failed with the given reason.
	Fail(ctx context.Context, reason string) error
}
