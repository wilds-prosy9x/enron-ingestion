package source_test

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"gocloud.dev/gcerrors"

	"github.com/wilds-prosy9x/enron-ingestion/internal/source"
)

func TestOpenFile(t *testing.T) {
	dir := t.TempDir()
	want := "archive contents"
	if err := os.WriteFile(filepath.Join(dir, "enron.tar.gz"), []byte(want), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	r, err := source.Open(t.Context(), "file://"+dir, "enron.tar.gz")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	if string(got) != want {
		t.Errorf("contents = %q, want %q", got, want)
	}
}

func TestOpenErrors(t *testing.T) {
	dir := t.TempDir()

	tests := []struct {
		name      string
		bucketURL string
		key       string
		notFound  bool
	}{
		{"missing key", "file://" + dir, "missing.tar.gz", true},
		{"missing directory", "file://" + filepath.Join(dir, "nope"), "enron.tar.gz", false},
		{"unknown scheme", "ftp://example.com/data", "enron.tar.gz", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := source.Open(t.Context(), tt.bucketURL, tt.key)
			if err == nil {
				r.Close()
				t.Fatal("Open returned nil error, want an error")
			}
			if got := errors.Is(err, source.ErrNotFound); got != tt.notFound {
				t.Errorf("errors.Is(err, ErrNotFound) = %v, want %v (err: %v)", got, tt.notFound, err)
			}
			// The driver's error must stay in the chain alongside ErrNotFound.
			if tt.notFound && gcerrors.Code(err) != gcerrors.NotFound {
				t.Errorf("gcerrors.Code(err) = %v, want NotFound (err: %v)", gcerrors.Code(err), err)
			}
		})
	}
}
