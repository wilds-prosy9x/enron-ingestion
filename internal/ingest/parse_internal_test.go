package ingest

import (
	"bytes"
	"testing"
)

func TestBodyNormaliser(t *testing.T) {
	tests := []struct{ in, want string }{
		{"", ""},
		{"a", "a"},
		{"a\r\nb\rc\nd", "a\nb\nc\nd"},
		{"a \t\r\nb  ", "a\nb"},
		{"a\n\r\n\n", "a"},
		{"a\n \n\t\nb", "a\n\n\nb"},
		{"\n  a  b\n", "\n  a  b"},
		{"a\r\r\nb", "a\n\nb"},
	}
	for _, tt := range tests {
		// Write the input in two parts split at every position, as io.Copy may.
		for i := range len(tt.in) + 1 {
			var buf bytes.Buffer
			n := &bodyNormaliser{w: &buf}
			n.Write([]byte(tt.in[:i]))
			n.Write([]byte(tt.in[i:]))
			if got := buf.String(); got != tt.want {
				t.Errorf("%q split at %d: got %q, want %q", tt.in, i, got, tt.want)
			}
		}
	}
}

func TestNormaliseAddresses(t *testing.T) {
	tests := []struct{ in, want string }{
		{"", ""},
		{"b@enron.com, A@Enron.com", "a@enron.com,b@enron.com"},
		{`"Doe, John" <John.Doe@enron.com>, john.doe@enron.com`, "john.doe@enron.com"},
		{"bob, Ann , , bob", "ann,bob"},
	}
	for _, tt := range tests {
		if got := normaliseAddresses(tt.in); got != tt.want {
			t.Errorf("normaliseAddresses(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}
