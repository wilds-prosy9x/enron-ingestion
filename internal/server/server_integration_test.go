package server_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/wilds-prosy9x/enron-ingestion/internal/server"
)

// TestStatusEndpoint starts a real HTTP server and calls the endpoint over the network.
func TestStatusEndpoint(t *testing.T) {
	ts := httptest.NewServer(server.NewHandler())
	t.Cleanup(ts.Close)

	resp, err := ts.Client().Get(ts.URL + "/status")
	if err != nil {
		t.Fatalf("GET /status: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want %q", ct, "application/json")
	}

	// Decode into a map so the test checks the exact JSON field names and values.
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	want := map[string]any{
		"state":                   "not_started",
		"processed_message_count": float64(0),
	}
	if !reflect.DeepEqual(body, want) {
		t.Errorf("body = %v, want %v", body, want)
	}
}

// TestRouting checks the status codes for supported and unsupported requests.
func TestRouting(t *testing.T) {
	ts := httptest.NewServer(server.NewHandler())
	t.Cleanup(ts.Close)

	tests := []struct {
		name   string
		method string
		path   string
		want   int
	}{
		{"get status", http.MethodGet, "/status", http.StatusOK},
		{"head status", http.MethodHead, "/status", http.StatusOK},
		{"post status", http.MethodPost, "/status", http.StatusMethodNotAllowed},
		{"put status", http.MethodPut, "/status", http.StatusMethodNotAllowed},
		{"delete status", http.MethodDelete, "/status", http.StatusMethodNotAllowed},
		{"unknown path", http.MethodGet, "/nope", http.StatusNotFound},
		{"root", http.MethodGet, "/", http.StatusNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequestWithContext(t.Context(), tt.method, ts.URL+tt.path, nil)
			if err != nil {
				t.Fatalf("new request: %v", err)
			}
			resp, err := ts.Client().Do(req)
			if err != nil {
				t.Fatalf("%s %s: %v", tt.method, tt.path, err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != tt.want {
				t.Errorf("%s %s: status = %d, want %d", tt.method, tt.path, resp.StatusCode, tt.want)
			}
		})
	}
}
