// Package source opens the dataset archive from local or cloud storage.
package source

import (
	"context"
	"errors"
	"fmt"
	"io"

	"gocloud.dev/blob"
	// Storage drivers, selected by the bucket URL's scheme.
	_ "gocloud.dev/blob/fileblob" // file:///path/to/dir
	_ "gocloud.dev/blob/gcsblob"  // gs://bucket
	_ "gocloud.dev/blob/s3blob"   // s3://bucket?region=...
	"gocloud.dev/gcerrors"
)

// ErrNotFound is returned by Open when the bucket has no object with the given key.
var ErrNotFound = errors.New("object not found")

// Open opens the object key in the bucket at bucketURL for streaming reads.
// The caller must close the returned reader.
//
// ctx is used for all reads on the returned reader, not just the open, so it
// must outlive them (e.g. the request context of the handler streaming the
// object). Whether cancelling ctx interrupts an in-flight read depends on the
// storage driver.
func Open(ctx context.Context, bucketURL, key string) (io.ReadCloser, error) {
	bucket, err := blob.OpenBucket(ctx, bucketURL)
	if err != nil {
		return nil, fmt.Errorf("open bucket %s: %w", bucketURL, err)
	}
	r, err := bucket.NewReader(ctx, key, nil)
	if err != nil {
		bucket.Close()
		if gcerrors.Code(err) == gcerrors.NotFound {
			return nil, fmt.Errorf("open %s in %s: %w: %w", key, bucketURL, ErrNotFound, err)
		}
		return nil, fmt.Errorf("open %s in %s: %w", key, bucketURL, err)
	}
	return &objectReader{Reader: r, bucket: bucket}, nil
}

// objectReader closes the bucket together with the object reader.
type objectReader struct {
	*blob.Reader
	bucket *blob.Bucket
}

func (r *objectReader) Close() error {
	return errors.Join(r.Reader.Close(), r.bucket.Close())
}
