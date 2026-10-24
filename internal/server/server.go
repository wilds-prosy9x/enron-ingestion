// Package server defines the HTTP routes and handlers for the REST API.
package server

import (
	"encoding/json"
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

func handleStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, StatusResponse{State: StateNotStarted, ProcessedMessageCount: 0})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
