package ingest

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/wilds-prosy9x/enron-ingestion/internal/store"
)

const (
	defaultBatchSize     = 1000
	defaultFlushInterval = 500 * time.Millisecond
	// endTimeout limits how long marking a run finished or failed may take.
	endTimeout = 5 * time.Second
)

// Recorder receives the results of a run. *store.Redis implements it.
type Recorder interface {
	RecordBatch(ctx context.Context, records []store.Record) error
	Finish(ctx context.Context) error
	Fail(ctx context.Context, reason string) error
}

// Runner reads all messages from Source and records them in batches.
type Runner struct {
	Source   Source
	Recorder Recorder
	// BatchSize is the maximum number of messages per RecordBatch call. Default 1000.
	BatchSize int
	// FlushInterval is the longest a message waits in a partial batch before
	// being recorded, so progress stays current when the source is slow. Default 500ms.
	FlushInterval time.Duration
}

// Run ingests all messages, then marks the run finished. If reading or
// recording fails, or ctx is cancelled, it marks the run failed and returns
// the error. Messages read before a failure are still recorded where possible.
// The caller must have started the run (store.Store.TryStart) beforehand.
func (r *Runner) Run(ctx context.Context) error {
	err := r.ingest(ctx)

	// ctx may already be cancelled, but the run's end must still be recorded.
	endCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), endTimeout)
	defer cancel()

	switch {
	case err == nil:
		if err := r.Recorder.Finish(endCtx); err != nil {
			return fmt.Errorf("finish: %w", err)
		}
		return nil
	case errors.Is(err, store.ErrNotRunning):
		// The run was ended elsewhere, so there is nothing to mark.
		return err
	default:
		if failErr := r.Recorder.Fail(endCtx, err.Error()); failErr != nil {
			return errors.Join(err, fmt.Errorf("mark failed: %w", failErr))
		}
		return err
	}
}

// ingest reads messages in a separate goroutine, so that a partial batch can be
// flushed while the source is waiting for more data.
func (r *Runner) ingest(ctx context.Context) error {
	batchSize := r.BatchSize
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}
	flushInterval := r.FlushInterval
	if flushInterval <= 0 {
		flushInterval = defaultFlushInterval
	}

	readCtx, stopReading := context.WithCancel(ctx)
	defer stopReading()
	msgs := make(chan Message, batchSize)
	readErr := make(chan error, 1)
	go func() {
		defer close(msgs)
		readErr <- r.Source.Messages(readCtx, func(m Message) error {
			select {
			case msgs <- m:
				return nil
			case <-readCtx.Done():
				return readCtx.Err()
			}
		})
	}()

	// stop ends the reader and returns err, or the read error if there is no err.
	stop := func(err error) error {
		stopReading()
		for range msgs {
			// Unblock the reader if it is sending.
		}
		if rerr := <-readErr; err == nil {
			return rerr
		}
		return err
	}

	batch := make([]store.Record, 0, batchSize)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		err := r.Recorder.RecordBatch(ctx, batch)
		batch = batch[:0]
		if err != nil {
			return fmt.Errorf("record batch: %w", err)
		}
		return nil
	}

	timer := time.NewTimer(flushInterval)
	timer.Stop()
	defer timer.Stop()
	for {
		select {
		case m, ok := <-msgs:
			if !ok {
				// The reader finished; record what it read even if it failed.
				return errors.Join(<-readErr, flush())
			}
			if len(batch) == 0 {
				timer.Reset(flushInterval)
			}
			batch = append(batch, store.Record{Key: m.Key, From: m.From})
			if len(batch) == batchSize {
				timer.Stop()
				if err := flush(); err != nil {
					return stop(err)
				}
			}
		case <-timer.C:
			if err := flush(); err != nil {
				return stop(err)
			}
		case <-ctx.Done():
			// Record everything read before the cancellation, including messages
			// still waiting in the channel, if the store still accepts them.
			stopReading()
			for m := range msgs {
				batch = append(batch, store.Record{Key: m.Key, From: m.From})
			}
			return stop(errors.Join(ctx.Err(), r.flushDetached(ctx, batch)))
		}
	}
}

// flushDetached records batch with a context that isn't cancelled along with ctx.
func (r *Runner) flushDetached(ctx context.Context, batch []store.Record) error {
	if len(batch) == 0 {
		return nil
	}
	flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), endTimeout)
	defer cancel()
	if err := r.Recorder.RecordBatch(flushCtx, batch); err != nil {
		return fmt.Errorf("record batch: %w", err)
	}
	return nil
}
