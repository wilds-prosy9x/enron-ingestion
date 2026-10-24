package redistest

import "testing"

func TestSafeName(t *testing.T) {
	tests := []struct{ name, want string }{
		{"TestLifecycle", "TestLifecycle"},
		{"TestRouting/GET_/status", "TestRouting/GET_/status"},
		{"TestX/a-b_c9", "TestX/a-b_c9"},
		{`TestX/[a]*b?\c`, "TestX/_a__b__c"},
		{"TestX/{tag}:key", "TestX/_tag__key"},
		{"TestX/ä", "TestX/_"},
	}
	for _, tt := range tests {
		if got := safeName(tt.name); got != tt.want {
			t.Errorf("safeName(%q) = %q, want %q", tt.name, got, tt.want)
		}
	}
}
