// Package server defines the HTTP routes and handlers for the REST API.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/wilds-prosy9x/enron-ingestion/internal/ingest"
	"github.com/wilds-prosy9x/enron-ingestion/internal/store"
)

// topSendersLimit is the number of senders returned by GET /top-senders.
const topSendersLimit = 10

// Store is the read side of the ingestion store. *store.Redis implements it.
type Store interface {
	Status(ctx context.Context) (store.Status, error)
	TopSenders(ctx context.Context, n int) ([]store.SenderCount, error)
}

// Starter starts an ingestion run. *ingest.Starter implements it.
type Starter interface {
	// Start returns store.ErrAlreadyStarted if a run is in progress or has
	// finished, and ingest.ErrStopped once the server is shutting down.
	Start(ctx context.Context) error
}

// NewHandler returns the root HTTP handler with all API routes registered.
func NewHandler(st Store, starter Starter) http.Handler {
	h := &handler{store: st, starter: starter}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /start", h.start)
	mux.HandleFunc("GET /status", h.status)
	mux.HandleFunc("GET /top-senders", h.topSenders)
	return mux
}

type handler struct {
	store   Store
	starter Starter
}

// StartResponse is the body returned by POST /start when a run starts.
type StartResponse struct {
	State store.State `json:"state"`
}

// ErrorResponse is the body returned when a request fails.
type ErrorResponse struct {
	Error string `json:"error"`
}

func (h *handler) start(w http.ResponseWriter, r *http.Request) {
	err := h.starter.Start(r.Context())
	switch {
	case err == nil:
		writeJSON(w, http.StatusAccepted, StartResponse{State: store.StateRunning})
	case errors.Is(err, store.ErrAlreadyStarted):
		writeJSON(w, http.StatusConflict, ErrorResponse{Error: "ingestion already started"})
	case errors.Is(err, ingest.ErrStopped):
		writeJSON(w, http.StatusServiceUnavailable, ErrorResponse{Error: "server is shutting down"})
	default:
		internalError(w, "start ingestion", err)
	}
}

// StatusResponse is the body returned by GET /status. It leaves out the store's
// Skipped count (distinct emails without a usable sender) on purpose: it is an
// internal detail, and unique_message_count already includes those emails.
type StatusResponse struct {
	State store.State `json:"state"`
	// DedupMode is the mode of the current or last run; omitted before the first run.
	DedupMode             string `json:"dedup_mode,omitempty"`
	ProcessedMessageCount int64  `json:"processed_message_count"`
	UniqueMessageCount    int64  `json:"unique_message_count"`
	// Error is the reason the run failed; omitted unless State is failed.
	Error string `json:"error,omitempty"`
}

func (h *handler) status(w http.ResponseWriter, r *http.Request) {
	st, err := h.store.Status(r.Context())
	if err != nil {
		internalError(w, "read status", err)
		return
	}
	writeJSON(w, http.StatusOK, StatusResponse{
		State:                 st.State,
		DedupMode:             st.DedupMode,
		ProcessedMessageCount: st.Processed,
		UniqueMessageCount:    st.Unique,
		Error:                 st.Error,
	})
}

// TopSendersResponse is the body returned by GET /top-senders.
type TopSendersResponse struct {
	Senders []SenderCount `json:"senders"`
}

// SenderCount is a sender and the number of distinct emails they sent.
type SenderCount struct {
	Email string `json:"email"`
	Count int64  `json:"count"`
}

func (h *handler) topSenders(w http.ResponseWriter, r *http.Request) {
	top, err := h.store.TopSenders(r.Context(), topSendersLimit)
	if err != nil {
		internalError(w, "read top senders", err)
		return
	}
	resp := TopSendersResponse{Senders: make([]SenderCount, len(top))}
	for i, sc := range top {
		resp.Senders[i] = SenderCount{Email: sc.Email, Count: sc.Count}
	}
	writeJSON(w, http.StatusOK, resp)
}

// internalError logs err and sends a generic 500 response, so that internal
// details such as connection addresses don't reach the client.
func internalError(w http.ResponseWriter, what string, err error) {
	slog.Error(what, "error", err)
	writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: "internal error"})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	// The status line has already been sent, so logging is all we can do.
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("write JSON response", "error", err)
	}
}
