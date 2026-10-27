package server_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/wilds-prosy9x/enron-ingestion/internal/ingest"
	"github.com/wilds-prosy9x/enron-ingestion/internal/server"
	"github.com/wilds-prosy9x/enron-ingestion/internal/store"
)

// startApp starts a real HTTP server with an empty store, reading messages from src.
func startApp(t *testing.T, src ingest.Source) string {
	t.Helper()
	st := store.NewMemory()
	starter := ingest.NewStarter(st, ingest.StarterConfig{Source: src, BatchSize: 1})
	ts := httptest.NewServer(server.NewHandler(st, starter, prometheus.NewRegistry()))
	// Cleanups run in reverse order: stop the server, then any run.
	t.Cleanup(starter.Stop)
	t.Cleanup(ts.Close)
	return ts.URL
}

// call sends a request over the network and returns the status code and decoded JSON body.
func call(t *testing.T, method, url string) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, url, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("%s %s: Content-Type = %q, want application/json", method, url, ct)
	}
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("%s %s: decode body: %v", method, url, err)
	}
	return resp.StatusCode, body
}

// waitForStatus polls GET /status until cond holds and returns that status.
func waitForStatus(t *testing.T, url string, cond func(map[string]any) bool) map[string]any {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		_, body := call(t, http.MethodGet, url+"/status")
		if cond(body) {
			return body
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for status; last: %v", body)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func processed(n int) func(map[string]any) bool {
	return func(b map[string]any) bool { return b["processed_message_count"] == float64(n) }
}

func stateIs(s string) func(map[string]any) bool {
	return func(b map[string]any) bool { return b["state"] == s }
}

// senders converts sender/count pairs into the expected /top-senders body.
func senders(pairs ...any) map[string]any {
	list := []any{}
	for i := 0; i < len(pairs); i += 2 {
		list = append(list, map[string]any{"email": pairs[i], "count": float64(pairs[i+1].(int))})
	}
	return map[string]any{"senders": list}
}

func checkBody(t *testing.T, what string, got, want map[string]any) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s = %v, want %v", what, got, want)
	}
}

func TestStatusBeforeStart(t *testing.T) {
	url := startApp(t, &chanSource{ch: make(chan ingest.Message)})

	code, body := call(t, http.MethodGet, url+"/status")
	if code != http.StatusOK {
		t.Errorf("status code = %d, want %d", code, http.StatusOK)
	}
	checkBody(t, "status", body, map[string]any{
		"state":                   "not_started",
		"processed_message_count": float64(0),
		"unique_message_count":    float64(0),
	})
	_, body = call(t, http.MethodGet, url+"/top-senders")
	checkBody(t, "top senders", body, senders())
}

// TestFullRun ingests a small archive end to end: storage, parsing, the store and the API.
func TestFullRun(t *testing.T) {
	dir := t.TempDir()
	m1 := []string{"From: a@enron.com", "Date: Mon, 1 Jan 2001 10:00:00 -0800", "Subject: one"}
	writeArchive(t, filepath.Join(dir, "enron.tar.gz"), map[string]string{
		"maildir/a/sent/1.":          email("Body", append(m1, "Message-ID: <1@x>", "X-Folder: sent")...),
		"maildir/a/all_documents/1.": email("Body", append(m1, "Message-ID: <2@x>", "X-Folder: all")...), // copy
		"maildir/a/sent/2.":          email("Two", "From: a@enron.com", "Subject: two"),
		"maildir/a/sent/3.":          email("Three", "From: A@Enron.com", "Subject: three"),
		"maildir/b/sent/1.":          email("Four", `From: "B" <b@enron.com>`, "Subject: four"),
		"maildir/b/sent/2.":          email("Five", "From: b@enron.com", "Subject: five"),
		"maildir/c/sent/1.":          email("Six", "From: c@enron.com", "Subject: six"),
		"maildir/c/inbox/1.":         email("Seven", "Subject: no sender"),
	})
	url := startApp(t, ingest.ArchiveSource{BucketURL: "file://" + dir, Key: "enron.tar.gz", Mode: ingest.DedupContent})

	code, body := call(t, http.MethodPost, url+"/start")
	if code != http.StatusAccepted {
		t.Fatalf("POST /start: status = %d, want %d (body %v)", code, http.StatusAccepted, body)
	}
	checkBody(t, "start", body, map[string]any{"state": "running"})

	body = waitForStatus(t, url, stateIs("finished"))
	checkBody(t, "status", body, map[string]any{
		"state":                   "finished",
		"dedup_mode":              "content",
		"processed_message_count": float64(8),
		"unique_message_count":    float64(7),
	})
	_, body = call(t, http.MethodGet, url+"/top-senders")
	checkBody(t, "top senders", body, senders("a@enron.com", 3, "b@enron.com", 2, "c@enron.com", 1))

	code, body = call(t, http.MethodPost, url+"/start")
	if code != http.StatusConflict {
		t.Errorf("second POST /start: status = %d, want %d (body %v)", code, http.StatusConflict, body)
	}
}

// TestPausedRun checks exact counts while a run is in progress, using a source
// that only yields messages when the test sends them.
func TestPausedRun(t *testing.T) {
	src := &chanSource{ch: make(chan ingest.Message)}
	url := startApp(t, src)
	if code, body := call(t, http.MethodPost, url+"/start"); code != http.StatusAccepted {
		t.Fatalf("POST /start: status = %d, want %d (body %v)", code, http.StatusAccepted, body)
	}

	src.send(t,
		ingest.Message{Key: "k1", From: "a@enron.com"},
		ingest.Message{Key: "k2", From: "b@enron.com"},
		ingest.Message{Key: "k3", From: "a@enron.com"},
	)
	// The source is paused now, so the counts can't move past 3.
	body := waitForStatus(t, url, processed(3))
	checkBody(t, "status after 3", body, map[string]any{
		"state":                   "running",
		"dedup_mode":              "content",
		"processed_message_count": float64(3),
		"unique_message_count":    float64(3),
	})
	_, body = call(t, http.MethodGet, url+"/top-senders")
	checkBody(t, "top senders after 3", body, senders("a@enron.com", 2, "b@enron.com", 1))

	src.send(t,
		ingest.Message{Key: "k4", From: "c@enron.com"},
		ingest.Message{Key: "k2", From: "b@enron.com"}, // duplicate
	)
	body = waitForStatus(t, url, processed(5))
	checkBody(t, "status after 5", body, map[string]any{
		"state":                   "running",
		"dedup_mode":              "content",
		"processed_message_count": float64(5),
		"unique_message_count":    float64(4),
	})

	close(src.ch)
	body = waitForStatus(t, url, stateIs("finished"))
	checkBody(t, "status at end", body, map[string]any{
		"state":                   "finished",
		"dedup_mode":              "content",
		"processed_message_count": float64(5),
		"unique_message_count":    float64(4),
	})
	_, body = call(t, http.MethodGet, url+"/top-senders")
	// Equal counts are in alphabetical order.
	checkBody(t, "top senders at end", body, senders("a@enron.com", 2, "b@enron.com", 1, "c@enron.com", 1))
}

// TestFailedRunCanRestart checks that a run that fails because the archive is
// missing reports the error, and that /start then works again.
func TestFailedRunCanRestart(t *testing.T) {
	dir := t.TempDir()
	url := startApp(t, ingest.ArchiveSource{BucketURL: "file://" + dir, Key: "enron.tar.gz", Mode: ingest.DedupContent})

	if code, body := call(t, http.MethodPost, url+"/start"); code != http.StatusAccepted {
		t.Fatalf("POST /start: status = %d, want %d (body %v)", code, http.StatusAccepted, body)
	}
	body := waitForStatus(t, url, stateIs("failed"))
	if msg := body["error"]; msg != "dataset archive not found" {
		t.Errorf("error = %v, want %q", msg, "dataset archive not found")
	}

	writeArchive(t, filepath.Join(dir, "enron.tar.gz"), map[string]string{
		"maildir/a/sent/1.": email("Body", "From: a@enron.com"),
	})
	if code, body := call(t, http.MethodPost, url+"/start"); code != http.StatusAccepted {
		t.Fatalf("POST /start after failure: status = %d, want %d (body %v)", code, http.StatusAccepted, body)
	}
	body = waitForStatus(t, url, stateIs("finished"))
	checkBody(t, "status", body, map[string]any{
		"state":                   "finished",
		"dedup_mode":              "content",
		"processed_message_count": float64(1),
		"unique_message_count":    float64(1),
	})
}

// chanSource yields messages sent on ch until it is closed.
type chanSource struct{ ch chan ingest.Message }

func (s *chanSource) DedupMode() ingest.DedupMode { return ingest.DedupContent }

func (s *chanSource) Messages(ctx context.Context, fn func(ingest.Message) error) error {
	for {
		select {
		case m, ok := <-s.ch:
			if !ok {
				return nil
			}
			if err := fn(m); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// send hands msgs to the source one by one. It fails the test if the source
// stops taking them, e.g. because the run ended early, instead of blocking forever.
func (s *chanSource) send(t *testing.T, msgs ...ingest.Message) {
	t.Helper()
	for _, m := range msgs {
		select {
		case s.ch <- m:
		case <-time.After(5 * time.Second):
			t.Fatalf("source didn't take message %s; has the run ended?", m.Key)
		}
	}
}

func email(body string, headers ...string) string {
	return strings.Join(headers, "\r\n") + "\r\n\r\n" + body
}

// writeArchive writes a gzipped tar archive holding files (path -> content) to path.
func writeArchive(t *testing.T, path string, files map[string]string) {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for name, content := range files {
		hdr := &tar.Header{Name: name, Typeflag: tar.TypeReg, Mode: 0o644, Size: int64(len(content))}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatalf("write header: %v", err)
		}
		if _, err := tw.Write([]byte(content)); err != nil {
			t.Fatalf("write file: %v", err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("close tar: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("close gzip: %v", err)
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatalf("write archive: %v", err)
	}
}
