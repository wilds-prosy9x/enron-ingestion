package ingest

import (
	"context"

	"github.com/wilds-prosy9x/enron-ingestion/internal/source"
)

// Source provides the messages to ingest.
type Source interface {
	// Messages calls fn for each message, stopping at the first error from fn.
	// It returns that error, or any error from reading the messages.
	Messages(ctx context.Context, fn func(Message) error) error
	// DedupMode is how the source recognises copies of the same email, i.e.
	// how it computes Message.Key. It is recorded with each run.
	DedupMode() DedupMode
}

// ArchiveSource reads messages from a gzipped tar archive in local or cloud storage.
type ArchiveSource struct {
	// BucketURL is the bucket holding the archive, e.g. file:///data or s3://bucket?region=eu-north-1.
	BucketURL string
	// Key is the archive's name in the bucket.
	Key string
	// Mode selects how copies of the same email are recognised.
	Mode DedupMode
}

func (s ArchiveSource) DedupMode() DedupMode { return s.Mode }

func (s ArchiveSource) Messages(ctx context.Context, fn func(Message) error) error {
	r, err := source.Open(ctx, s.BucketURL, s.Key)
	if err != nil {
		return err
	}
	defer r.Close()
	return Parse(r, s.Mode, func(m Message) error {
		// Not every storage driver stops reads when ctx is cancelled, so check here too.
		if err := ctx.Err(); err != nil {
			return err
		}
		return fn(m)
	})
}
