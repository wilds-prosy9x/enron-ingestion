package ingest_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/wilds-prosy9x/enron-ingestion/internal/ingest"
)

type file struct {
	path    string
	content string
}

// buildArchive returns a gzipped tar archive with a directory entry per file plus the files themselves.
func buildArchive(t *testing.T, files []file) []byte {
	t.Helper()
	return gzipData(t, buildTar(t, files))
}

// buildTar returns an uncompressed tar archive with a directory entry per file plus the files themselves.
func buildTar(t *testing.T, files []file) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, f := range files {
		dir := f.path[:strings.LastIndex(f.path, "/")+1]
		if err := tw.WriteHeader(&tar.Header{Name: dir, Typeflag: tar.TypeDir, Mode: 0o755}); err != nil {
			t.Fatalf("write dir header: %v", err)
		}
		if err := tw.WriteHeader(&tar.Header{Name: f.path, Typeflag: tar.TypeReg, Mode: 0o644, Size: int64(len(f.content))}); err != nil {
			t.Fatalf("write file header: %v", err)
		}
		if _, err := tw.Write([]byte(f.content)); err != nil {
			t.Fatalf("write file: %v", err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("close tar: %v", err)
	}
	return buf.Bytes()
}

// gzipData returns data compressed with gzip.
func gzipData(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(data); err != nil {
		t.Fatalf("write gzip: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("close gzip: %v", err)
	}
	return buf.Bytes()
}

// email builds an email with the given headers and body.
func email(body string, headers ...string) string {
	return strings.Join(headers, "\r\n") + "\r\n\r\n" + body
}

// parseAll parses the archive and returns the messages keyed by path.
func parseAll(t *testing.T, archive []byte, mode ingest.DedupMode) map[string]ingest.Message {
	t.Helper()
	got := map[string]ingest.Message{}
	err := ingest.Parse(bytes.NewReader(archive), mode, func(m ingest.Message) error {
		got[m.Path] = m
		return nil
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	return got
}

func TestParseSender(t *testing.T) {
	archive := buildArchive(t, []file{
		{"maildir/a/inbox/1.", email("Hi", "Message-ID: <1@x>", "From: fran.fagan@enron.com")},
		{"maildir/a/inbox/2.", email("Hi", "Message-ID: <2@x>", `From: "Fagan, Fran" <fran.fagan@enron.com>`)},
		{"maildir/a/inbox/3.", email("Hi", "Message-ID: <3@x>", "From: Kenneth.Lay@ENRON.com")},
		{"maildir/a/inbox/4.", email("Hi", "Message-ID: <4@x>", "From: ")},
		{"maildir/a/inbox/5.", email("Hi", "Message-ID: <5@x>")},
		// Not a valid address, so the raw value is kept.
		{"maildir/a/inbox/6.", email("Hi", "Message-ID: <6@x>", "From: .Jeff@Enron.com")},
		{"maildir/a/inbox/7.", "not an email"},
	})
	want := map[string]string{
		"maildir/a/inbox/1.": "fran.fagan@enron.com",
		"maildir/a/inbox/2.": "fran.fagan@enron.com",
		"maildir/a/inbox/3.": "kenneth.lay@enron.com",
		"maildir/a/inbox/4.": "",
		"maildir/a/inbox/5.": "",
		"maildir/a/inbox/6.": ".jeff@enron.com",
		"maildir/a/inbox/7.": "",
	}

	for _, mode := range []ingest.DedupMode{ingest.DedupMessageID, ingest.DedupContent} {
		t.Run(string(mode), func(t *testing.T) {
			got := parseAll(t, archive, mode)
			if len(got) != len(want) {
				t.Fatalf("got %d messages, want %d", len(got), len(want))
			}
			for path, from := range want {
				if got[path].From != from {
					t.Errorf("%s: From = %q, want %q", path, got[path].From, from)
				}
			}
		})
	}
}

func TestParseKeys(t *testing.T) {
	headers := []string{"From: fran.fagan@enron.com", "Date: Fri, 14 Sep 2001 14:05:43 -0700 (PDT)", "Subject: Promotions", "To: lynn.blair@enron.com"}
	withID := func(id string, extra ...string) []string {
		return append(append([]string{"Message-ID: " + id}, headers...), extra...)
	}

	archive := buildArchive(t, []file{
		{"maildir/a/sent/1.", email("Body", withID("<1@x>", "X-Folder: sent")...)},
		// Same email stored in another folder: new Message-ID, different X- headers.
		{"maildir/b/inbox/1.", email("Body", withID("<2@x>", "X-Folder: inbox")...)},
		// Same Message-ID as the first, but a different body.
		{"maildir/c/inbox/1.", email("Other body", withID("<1@x>")...)},
		// Same as the first except for the subject.
		{"maildir/d/inbox/1.", email("Body", "Message-ID: <3@x>", headers[0], headers[1], "Subject: Other", headers[3])},
		// No Message-ID.
		{"maildir/e/inbox/1.", email("Body", headers...)},
		{"maildir/e/inbox/2.", email("Body", headers...)},
		// Unreadable headers.
		{"maildir/f/inbox/1.", "not an email"},
		{"maildir/f/inbox/2.", "not an email"},
	})

	tests := []struct {
		mode ingest.DedupMode
		// Paths in the same group must share a key; paths in different groups must not.
		groups [][]string
	}{
		{ingest.DedupMessageID, [][]string{
			{"maildir/a/sent/1.", "maildir/c/inbox/1."},
			{"maildir/b/inbox/1."},
			{"maildir/d/inbox/1."},
			{"maildir/e/inbox/1."},
			{"maildir/e/inbox/2."},
			{"maildir/f/inbox/1."},
			{"maildir/f/inbox/2."},
		}},
		{ingest.DedupContent, [][]string{
			{"maildir/a/sent/1.", "maildir/b/inbox/1.", "maildir/e/inbox/1.", "maildir/e/inbox/2."},
			{"maildir/c/inbox/1."},
			{"maildir/d/inbox/1."},
			{"maildir/f/inbox/1."},
			{"maildir/f/inbox/2."},
		}},
	}
	for _, tt := range tests {
		t.Run(string(tt.mode), func(t *testing.T) {
			got := parseAll(t, archive, tt.mode)

			groupOf := map[string]int{}
			for i, group := range tt.groups {
				for _, path := range group {
					groupOf[path] = i
				}
			}
			for path := range groupOf {
				if _, ok := got[path]; !ok {
					t.Errorf("%s: not parsed", path)
				}
			}
			for path := range got {
				// An ungrouped path would compare as group 0, so the key checks below would be wrong.
				if _, ok := groupOf[path]; !ok {
					t.Fatalf("%s: parsed but not in any group", path)
				}
			}
			for p1, m1 := range got {
				if len(m1.Key) != 32 {
					t.Errorf("%s: key %q has length %d, want 32", p1, m1.Key, len(m1.Key))
				}
				for p2, m2 := range got {
					if p1 >= p2 {
						continue
					}
					sameGroup := groupOf[p1] == groupOf[p2]
					sameKey := m1.Key == m2.Key
					if sameGroup && !sameKey {
						t.Errorf("%s and %s: keys differ, want equal", p1, p2)
					}
					if !sameGroup && sameKey {
						t.Errorf("%s and %s: keys equal, want different", p1, p2)
					}
				}
			}
		})
	}
}

func TestParseStopsOnCallbackError(t *testing.T) {
	archive := buildArchive(t, []file{
		{"maildir/a/inbox/1.", email("Hi", "Message-ID: <1@x>", "From: a@enron.com")},
		{"maildir/a/inbox/2.", email("Hi", "Message-ID: <2@x>", "From: a@enron.com")},
	})
	stop := errors.New("stop")

	calls := 0
	err := ingest.Parse(bytes.NewReader(archive), ingest.DedupContent, func(ingest.Message) error {
		calls++
		return stop
	})
	if !errors.Is(err, stop) {
		t.Errorf("err = %v, want %v", err, stop)
	}
	if calls != 1 {
		t.Errorf("callback called %d times, want 1", calls)
	}
}

func TestParseTruncatedFile(t *testing.T) {
	content := email("Body text", "Message-ID: <1@x>", "From: a@enron.com", "Subject: Hi")
	tarData := buildTar(t, []file{{"maildir/a/inbox/1.", content}})
	start := bytes.Index(tarData, []byte(content))
	if start < 0 {
		t.Fatal("file content not found in tar")
	}

	cuts := []struct {
		name string
		at   int
	}{
		{"in headers", strings.Index(content, "Subject")},
		{"in body", strings.Index(content, "text")},
	}
	for _, cut := range cuts {
		archive := gzipData(t, tarData[:start+cut.at])
		for _, mode := range []ingest.DedupMode{ingest.DedupMessageID, ingest.DedupContent} {
			t.Run(cut.name+"/"+string(mode), func(t *testing.T) {
				calls := 0
				err := ingest.Parse(bytes.NewReader(archive), mode, func(ingest.Message) error {
					calls++
					return nil
				})
				if !errors.Is(err, io.ErrUnexpectedEOF) {
					t.Errorf("err = %v, want %v", err, io.ErrUnexpectedEOF)
				}
				if calls != 0 {
					t.Errorf("callback called %d times, want 0", calls)
				}
			})
		}
	}
}

func TestParseErrors(t *testing.T) {
	valid := buildArchive(t, []file{{"maildir/a/inbox/1.", email("Hi", "From: a@enron.com")}})
	tests := []struct {
		name string
		data []byte
		mode ingest.DedupMode
		// want is the error Parse must wrap.
		want error
	}{
		{"not gzip", []byte("plain text"), ingest.DedupContent, gzip.ErrHeader},
		// Cut inside the first tar header, before any file.
		{"truncated tar header", valid[:40], ingest.DedupContent, io.ErrUnexpectedEOF},
		{"unknown mode", valid, ingest.DedupMode("subject"), ingest.ErrUnknownMode},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			err := ingest.Parse(bytes.NewReader(tt.data), tt.mode, func(ingest.Message) error {
				calls++
				return nil
			})
			if !errors.Is(err, tt.want) {
				t.Errorf("err = %v, want %v", err, tt.want)
			}
			if calls != 0 {
				t.Errorf("callback called %d times, want 0", calls)
			}
		})
	}
}
