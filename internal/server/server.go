// Package server defines the HTTP routes and handlers for the REST API.
package server

import (
	"encoding/json"
	"log/slog"
	"net/http"
)

// NewHandler returns the root HTTP handler with all API routes registered.
func NewHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /status", handleStatus)
	return mux
}

// IngestionState is the lifecycle state of the ingestion process.
type IngestionState string

const (
	StateNotStarted IngestionState = "not_started"
	StateRunning    IngestionState = "running"
	StateFinished   IngestionState = "finished"
	StateFailed     IngestionState = "failed"
)

// StatusResponse is the body returned by GET /status.
type StatusResponse struct {
	State                 IngestionState `json:"state"`
	ProcessedMessageCount int            `json:"processed_message_count"`
}

func handleStatus(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, StatusResponse{State: StateNotStarted, ProcessedMessageCount: 0})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	// The status line has already been sent, so logging is all we can do.
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("write JSON response", "error", err)
	}
}
