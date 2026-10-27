package server_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/wilds-prosy9x/enron-ingestion/internal/ingest"
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
	h := server.NewHandler(&fakeStore{}, &fakeStarter{}, prometheus.NewRegistry())

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
		{http.MethodGet, "/healthz", http.StatusOK},
		{http.MethodHead, "/healthz", http.StatusOK},
		{http.MethodPost, "/healthz", http.StatusMethodNotAllowed},
		{http.MethodGet, "/readyz", http.StatusOK},
		{http.MethodHead, "/readyz", http.StatusOK},
		{http.MethodPost, "/readyz", http.StatusMethodNotAllowed},
		{http.MethodGet, "/metrics", http.StatusOK},
		{http.MethodPost, "/metrics", http.StatusMethodNotAllowed},
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

func TestStartRefused(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		wantCode int
		wantErr  string
	}{
		{"already started", store.ErrAlreadyStarted, http.StatusConflict, "ingestion already started"},
		{"shutting down", ingest.ErrStopped, http.StatusServiceUnavailable, "server is shutting down"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := server.NewHandler(&fakeStore{}, &fakeStarter{err: tt.err}, prometheus.NewRegistry())

			code, body := do(t, h, http.MethodPost, "/start")
			if code != tt.wantCode {
				t.Errorf("status = %d, want %d", code, tt.wantCode)
			}
			if want := map[string]any{"error": tt.wantErr}; !reflect.DeepEqual(body, want) {
				t.Errorf("body = %v, want %v", body, want)
			}
		})
	}
}

func TestStatusFailedRun(t *testing.T) {
	h := server.NewHandler(&fakeStore{status: store.Status{
		State: store.StateFailed, DedupMode: "content", Processed: 5, Unique: 4, Skipped: 1, Error: "archive truncated",
	}}, &fakeStarter{}, prometheus.NewRegistry())

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
	h := server.NewHandler(&fakeStore{}, &fakeStarter{}, prometheus.NewRegistry())

	// An empty list must be [], not null.
	_, body := do(t, h, http.MethodGet, "/top-senders")
	if want := map[string]any{"senders": []any{}}; !reflect.DeepEqual(body, want) {
		t.Errorf("body = %v, want %v", body, want)
	}
}

func TestHealthAndReadiness(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		storeErr error
		wantCode int
		wantBody map[string]any
	}{
		{"healthy", "/healthz", nil, http.StatusOK, map[string]any{"status": "ok"}},
		// Liveness ignores the store, so a store outage doesn't restart the process.
		{"healthy with store down", "/healthz", errors.New("down"), http.StatusOK, map[string]any{"status": "ok"}},
		{"ready", "/readyz", nil, http.StatusOK, map[string]any{"status": "ready"}},
		{"not ready", "/readyz", errors.New("dial tcp 10.0.0.5:6379: connection refused"),
			http.StatusServiceUnavailable, map[string]any{"error": "store unavailable"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := server.NewHandler(&fakeStore{err: tt.storeErr}, &fakeStarter{}, prometheus.NewRegistry())

			code, body := do(t, h, http.MethodGet, tt.path)
			if code != tt.wantCode {
				t.Errorf("status = %d, want %d", code, tt.wantCode)
			}
			if !reflect.DeepEqual(body, tt.wantBody) {
				t.Errorf("body = %v, want %v", body, tt.wantBody)
			}
		})
	}
}

func TestMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	c := prometheus.NewCounter(prometheus.CounterOpts{Name: "test_total", Help: "A test counter."})
	reg.MustRegister(c)
	c.Add(3)
	h := server.NewHandler(&fakeStore{}, &fakeStarter{}, reg)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("Content-Type = %q, want text/plain", ct)
	}
	if want := "test_total 3\n"; !strings.Contains(rec.Body.String(), want) {
		t.Errorf("body doesn't contain %q:\n%s", want, rec.Body)
	}
}

// TestInternalErrors checks that store and starter errors give a generic 500
// without leaking the error's text.
func TestInternalErrors(t *testing.T) {
	secret := errors.New("dial tcp 10.0.0.5:6379: connection refused")
	h := server.NewHandler(&fakeStore{err: secret}, &fakeStarter{err: secret}, prometheus.NewRegistry())

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
