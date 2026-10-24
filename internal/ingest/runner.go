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
	// endTimeout limits how long recording after a cancellation, and marking a
	// run finished or failed, may take.
	endTimeout = 5 * time.Second
)

// Recorder receives the results of a run. store.Run implements it.
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
// recording fails, or ctx is cancelled before all messages are read, it marks
// the run failed and returns the error. Messages read before a failure are
// still recorded where possible; a batch already being recorded when ctx is
// cancelled may be lost, depending on the Recorder.
// After cancellation, Run waits for the Source to stop; a Source that ignores
// ctx while blocked in a read delays it until that read returns.
// The Recorder is usually the store.Run returned by store.Store.TryStart.
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
			// Checked first, as the select below picks at random when both are ready.
			if err := readCtx.Err(); err != nil {
				return err
			}
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
		err := r.record(ctx, batch)
		batch = batch[:0]
		return err
	}

	timer := time.NewTimer(flushInterval)
	timer.Stop()
	defer timer.Stop()
	done := ctx.Done()
	for {
		select {
		case m, ok := <-msgs:
			if !ok {
				// The reader finished; record what it read even if it failed
				// or ctx was cancelled.
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
		case <-done:
			// Stop reading, but keep recording what was already read, including
			// messages still waiting in the channel, until the reader returns.
			stopReading()
			done = nil
		}
	}
}

// record records batch. Once ctx is cancelled it uses a context that isn't,
// so that messages read before the cancellation are still recorded.
func (r *Runner) record(ctx context.Context, batch []store.Record) error {
	if ctx.Err() != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.WithoutCancel(ctx), endTimeout)
		defer cancel()
	}
	if err := r.Recorder.RecordBatch(ctx, batch); err != nil {
		return fmt.Errorf("record batch: %w", err)
	}
	return nil
}
