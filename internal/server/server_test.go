package server_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/wilds-prosy9x/enron-ingestion/internal/server"
	"github.com/wilds-prosy9x/enron-ingestion/internal/store"
)

type fakeStore struct {
	status store.Status
	top    []store.SenderCount
	err    error
}

func (f *fakeStore) Status(context.Context) (store.Status, error) { return f.status, f.err }

func (f *fakeStore) TopSenders(_ context.Context, n int) ([]store.SenderCount, error) {
	if len(f.top) > n {
		return f.top[:n], f.err
	}
	return f.top, f.err
}

type fakeStarter struct{ err error }

func (f *fakeStarter) Start(context.Context) error { return f.err }

// do sends a request to h and returns the status code and the decoded JSON body.
func do(t *testing.T, h http.Handler, method, path string) (int, map[string]any) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), method, path, nil))
	var body map[string]any
	if rec.Body.Len() > 0 && rec.Header().Get("Content-Type") == "application/json" {
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("%s %s: decode body %q: %v", method, path, rec.Body, err)
		}
	}
	return rec.Code, body
}

// TestRouting checks the status codes for supported and unsupported requests.
func TestRouting(t *testing.T) {
	h := server.NewHandler(&fakeStore{}, &fakeStarter{})

	tests := []struct {
		method string
		path   string
		want   int
	}{
		{http.MethodPost, "/start", http.StatusAccepted},
		{http.MethodGet, "/start", http.StatusMethodNotAllowed},
		{http.MethodGet, "/status", http.StatusOK},
		{http.MethodHead, "/status", http.StatusOK},
		{http.MethodPost, "/status", http.StatusMethodNotAllowed},
		{http.MethodPut, "/status", http.StatusMethodNotAllowed},
		{http.MethodDelete, "/status", http.StatusMethodNotAllowed},
		{http.MethodGet, "/top-senders", http.StatusOK},
		{http.MethodPost, "/top-senders", http.StatusMethodNotAllowed},
		{http.MethodGet, "/nope", http.StatusNotFound},
		{http.MethodGet, "/", http.StatusNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.method+" "+tt.path, func(t *testing.T) {
			if got, _ := do(t, h, tt.method, tt.path); got != tt.want {
				t.Errorf("status = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestStartConflict(t *testing.T) {
	h := server.NewHandler(&fakeStore{}, &fakeStarter{err: store.ErrAlreadyStarted})

	code, body := do(t, h, http.MethodPost, "/start")
	if code != http.StatusConflict {
		t.Errorf("status = %d, want %d", code, http.StatusConflict)
	}
	if want := map[string]any{"error": "ingestion already started"}; !reflect.DeepEqual(body, want) {
		t.Errorf("body = %v, want %v", body, want)
	}
}

func TestStatusFailedRun(t *testing.T) {
	h := server.NewHandler(&fakeStore{status: store.Status{
		State: store.StateFailed, DedupMode: "content", Processed: 5, Unique: 4, Skipped: 1, Error: "archive truncated",
	}}, &fakeStarter{})

	_, body := do(t, h, http.MethodGet, "/status")
	want := map[string]any{
		"state":                   "failed",
		"dedup_mode":              "content",
		"processed_message_count": float64(5),
		"unique_message_count":    float64(4),
		"error":                   "archive truncated",
	}
	if !reflect.DeepEqual(body, want) {
		t.Errorf("body = %v, want %v", body, want)
	}
}

func TestTopSendersEmpty(t *testing.T) {
	h := server.NewHandler(&fakeStore{}, &fakeStarter{})

	// An empty list must be [], not null.
	_, body := do(t, h, http.MethodGet, "/top-senders")
	if want := map[string]any{"senders": []any{}}; !reflect.DeepEqual(body, want) {
		t.Errorf("body = %v, want %v", body, want)
	}
}

// TestInternalErrors checks that store and starter errors give a generic 500
// without leaking the error's text.
func TestInternalErrors(t *testing.T) {
	secret := errors.New("dial tcp 10.0.0.5:6379: connection refused")
	h := server.NewHandler(&fakeStore{err: secret}, &fakeStarter{err: secret})

	for _, req := range []struct{ method, path string }{
		{http.MethodPost, "/start"},
		{http.MethodGet, "/status"},
		{http.MethodGet, "/top-senders"},
	} {
		t.Run(req.method+" "+req.path, func(t *testing.T) {
			code, body := do(t, h, req.method, req.path)
			if code != http.StatusInternalServerError {
				t.Errorf("status = %d, want %d", code, http.StatusInternalServerError)
			}
			if want := map[string]any{"error": "internal error"}; !reflect.DeepEqual(body, want) {
				t.Errorf("body = %v, want %v", body, want)
			}
		})
	}
}
