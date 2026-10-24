package server_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"enron_ingestion/internal/server"
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

func TestStatusEndpointRejectsOtherMethods(t *testing.T) {
	ts := httptest.NewServer(server.NewHandler())
	t.Cleanup(ts.Close)

	resp, err := ts.Client().Post(ts.URL+"/status", "application/json", nil)
	if err != nil {
		t.Fatalf("POST /status: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusMethodNotAllowed)
	}
}
