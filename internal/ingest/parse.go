// Package ingest reads the Enron email archive and extracts the data needed for ingestion.
package ingest

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/mail"
	"slices"
	"strings"
)

// DedupMode selects how copies of the same email are recognised.
type DedupMode string

const (
	// DedupMessageID treats emails with the same Message-ID header as copies.
	// Use it for archives that kept their original Message-IDs.
	DedupMessageID DedupMode = "message_id"
	// DedupContent treats emails with the same sender, date, subject, recipients
	// and body as copies. Use it when Message-IDs aren't reliable, as in the
	// Enron dataset, where every stored copy was given a new Message-ID.
	// The order and case of recipients, line endings, whitespace at the end of
	// lines and trailing blank lines don't matter.
	DedupContent DedupMode = "content"
)

var (
	// ErrUnknownMode is returned by Parse when given a DedupMode it doesn't support.
	ErrUnknownMode = errors.New("unknown dedup mode")
	// ErrReadArchive is wrapped by the errors Parse returns when the archive is
	// corrupt or truncated, or reading it fails.
	ErrReadArchive = errors.New("read archive")
)

// Message holds the fields extracted from one email in the archive.
type Message struct {
	// Path is the file's path inside the archive.
	Path string
	// Key identifies the email for deduplication: copies of the same email have
	// the same Key. Emails that can't be identified get a key derived from Path.
	Key string
	// From is the normalised sender address, or empty if no usable sender was found.
	From string
}

// Parse reads a gzipped tar archive of emails and calls fn for each regular file in it.
// Emails with unreadable headers are still passed to fn, with an empty From.
// Parsing stops at the first error returned by fn.
func Parse(r io.Reader, mode DedupMode, fn func(Message) error) error {
	if mode != DedupMessageID && mode != DedupContent {
		return fmt.Errorf("%w %q", ErrUnknownMode, mode)
	}
	gz, err := gzip.NewReader(r)
	if err != nil {
		return fmt.Errorf("%w: open gzip: %w", ErrReadArchive, err)
	}
	defer gz.Close()

	h := sha256.New()
	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("%w: read tar: %w", ErrReadArchive, err)
		}
		if hdr.Typeflag != tar.TypeReg {
			continue
		}
		er := &errReader{r: tr}
		msg, err := parseMessage(hdr.Name, er, mode, h)
		if err != nil {
			return err
		}
		// Read the rest of the file so that a truncated archive is reported
		// before fn sees an incomplete email.
		_, _ = io.Copy(io.Discard, er)
		if er.err != nil {
			return fmt.Errorf("%w: read %s: %w", ErrReadArchive, hdr.Name, er.err)
		}
		if err := fn(msg); err != nil {
			return err
		}
	}
}

func parseMessage(path string, r io.Reader, mode DedupMode, h hash.Hash) (Message, error) {
	msg := Message{Path: path, Key: hashFields(h, "path", path)}
	m, err := mail.ReadMessage(r)
	if err != nil {
		// Malformed headers: keep the message with its path-based key.
		// Read errors are caught by the caller's errReader.
		return msg, nil
	}
	msg.From = normaliseSender(m.Header.Get("From"))

	switch mode {
	case DedupMessageID:
		if id := strings.TrimSpace(m.Header.Get("Message-ID")); id != "" {
			msg.Key = hashFields(h, "id", id)
		}
	case DedupContent:
		h.Reset()
		writeFields(h, "content", msg.From,
			strings.TrimSpace(m.Header.Get("Date")),
			strings.TrimSpace(m.Header.Get("Subject")),
			normaliseAddresses(m.Header.Get("To")),
			normaliseAddresses(m.Header.Get("Cc")))
		// An error here means the archive itself is broken, so stop.
		if _, err := io.Copy(&bodyNormaliser{w: h}, m.Body); err != nil {
			return Message{}, fmt.Errorf("%w: read %s: %w", ErrReadArchive, path, err)
		}
		msg.Key = sumKey(h)
	}
	return msg, nil
}

// errReader records the first read error other than io.EOF, so that it isn't
// lost when a consumer like mail.ReadMessage treats it as malformed input.
type errReader struct {
	r   io.Reader
	err error
}

func (e *errReader) Read(p []byte) (int, error) {
	n, err := e.r.Read(p)
	if err != nil && !errors.Is(err, io.EOF) && e.err == nil {
		e.err = err
	}
	return n, err
}

// hashFields returns a key for the given fields.
func hashFields(h hash.Hash, fields ...string) string {
	h.Reset()
	writeFields(h, fields...)
	return sumKey(h)
}

// sumKey returns a 128-bit hex key for the data written to h.
// Keys have a fixed length so their storage cost doesn't depend on the input.
func sumKey(h hash.Hash) string {
	return hex.EncodeToString(h.Sum(nil)[:16])
}

// writeFields writes each field followed by a NUL byte, so that
// different field splits like ("ab", "c") and ("a", "bc") hash differently.
func writeFields(h hash.Hash, fields ...string) {
	for _, f := range fields {
		io.WriteString(h, f)
		h.Write([]byte{0})
	}
}

// normaliseSender returns the lowercased email address from a From header value.
// If the value isn't a valid address, the trimmed raw value is used instead.
// The whole address is lowercased, although RFC 5321 lets the part before
// the @ be case-sensitive, because in practice mail servers ignore its case.
func normaliseSender(from string) string {
	if addr, err := mail.ParseAddress(from); err == nil {
		return strings.ToLower(addr.Address)
	}
	return strings.ToLower(strings.TrimSpace(from))
}

// normaliseAddresses returns the addresses in an address list header value,
// lowercased like normaliseSender, sorted and without duplicates, so that
// copies of an email listing the same recipients differently get the same key.
// If the list isn't valid, it is split at commas instead.
func normaliseAddresses(list string) string {
	var addrs []string
	if parsed, err := mail.ParseAddressList(list); err == nil {
		for _, a := range parsed {
			addrs = append(addrs, strings.ToLower(a.Address))
		}
	} else {
		for _, a := range strings.Split(list, ",") {
			if a = strings.ToLower(strings.TrimSpace(a)); a != "" {
				addrs = append(addrs, a)
			}
		}
	}
	slices.Sort(addrs)
	return strings.Join(slices.Compact(addrs), ",")
}

// bodyNormaliser writes an email body to w with line endings unified to \n,
// spaces and tabs at the end of lines removed and trailing blank lines dropped,
// so that copies of an email differing only in that whitespace get the same key.
// Whitespace is held back until a later character shows it isn't trailing.
type bodyNormaliser struct {
	w        io.Writer
	spaces   []byte // spaces and tabs since the last line break or other character
	newlines int    // line breaks held back
	afterCR  bool   // the last byte was \r, so a \n right after it is part of the same line break
	out      []byte // reused output buffer
}

func (n *bodyNormaliser) Write(p []byte) (int, error) {
	n.out = n.out[:0]
	for _, c := range p {
		afterCR := n.afterCR
		n.afterCR = c == '\r'
		switch c {
		case ' ', '\t':
			n.spaces = append(n.spaces, c)
		case '\n':
			if afterCR {
				continue
			}
			fallthrough
		case '\r':
			n.spaces = n.spaces[:0]
			n.newlines++
		default:
			for range n.newlines {
				n.out = append(n.out, '\n')
			}
			n.newlines = 0
			n.out = append(n.out, n.spaces...)
			n.spaces = n.spaces[:0]
			n.out = append(n.out, c)
		}
	}
	if _, err := n.w.Write(n.out); err != nil {
		return 0, err
	}
	return len(p), nil
}
