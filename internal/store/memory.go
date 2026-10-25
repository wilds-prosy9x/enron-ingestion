package store

import (
	"cmp"
	"context"
	"slices"
	"sync"
)

// Memory is a Store that keeps its data in memory, so it is lost when the
// process exits.
type Memory struct {
	mu      sync.Mutex
	status  Status
	run     *memoryRun          // the current run; nil before the first one
	seen    map[string]struct{} // dedup keys seen so far
	senders map[string]int64    // sender -> count of distinct emails
}

var _ Store = (*Memory)(nil)

// NewMemory returns an empty Memory store.
func NewMemory() *Memory {
	return &Memory{status: Status{State: StateNotStarted}}
}

func (m *Memory) TryStart(_ context.Context, dedupMode string) (Run, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s := m.status.State; s == StateRunning || s == StateFinished {
		return nil, ErrAlreadyStarted
	}
	m.run = &memoryRun{m: m}
	m.status = Status{State: StateRunning, DedupMode: dedupMode}
	m.seen = make(map[string]struct{})
	m.senders = make(map[string]int64)
	return m.run, nil
}

// memoryRun is a Run of a Memory store. Its methods only act while it is the
// store's current run and in progress, so a failed run can't change the run
// that replaced it.
type memoryRun struct {
	m *Memory
}

var _ Run = (*memoryRun)(nil)

// active reports whether run can change the store. run.m.mu must be held.
func (run *memoryRun) active() bool {
	return run.m.run == run && run.m.status.State == StateRunning
}

func (run *memoryRun) RecordBatch(_ context.Context, records []Record) error {
	if len(records) == 0 {
		return nil
	}
	m := run.m
	m.mu.Lock()
	defer m.mu.Unlock()
	if !run.active() {
		return ErrNotRunning
	}
	for _, rec := range records {
		if _, ok := m.seen[rec.Key]; ok {
			continue
		}
		m.seen[rec.Key] = struct{}{}
		m.status.Unique++
		if rec.From == "" {
			m.status.Skipped++
		} else {
			m.senders[rec.From]++
		}
	}
	m.status.Processed += int64(len(records))
	return nil
}

func (run *memoryRun) Finish(context.Context) error {
	return run.end(StateFinished, "")
}

func (run *memoryRun) Fail(_ context.Context, reason string) error {
	return run.end(StateFailed, reason)
}

func (run *memoryRun) end(state State, reason string) error {
	m := run.m
	m.mu.Lock()
	defer m.mu.Unlock()
	if !run.active() {
		return ErrNotRunning
	}
	m.status.State = state
	m.status.Error = reason
	return nil
}

func (m *Memory) Status(context.Context) (Status, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.status, nil
}

func (m *Memory) TopSenders(_ context.Context, n int) ([]SenderCount, error) {
	if n <= 0 {
		return []SenderCount{}, nil
	}
	m.mu.Lock()
	out := make([]SenderCount, 0, len(m.senders))
	for email, count := range m.senders {
		out = append(out, SenderCount{Email: email, Count: count})
	}
	m.mu.Unlock()
	// Senders with equal counts are returned in alphabetical order.
	slices.SortFunc(out, func(a, b SenderCount) int {
		return cmp.Or(cmp.Compare(b.Count, a.Count), cmp.Compare(a.Email, b.Email))
	})
	return out[:min(n, len(out))], nil
}
