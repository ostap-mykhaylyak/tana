package config

import "testing"

func TestMatchGlob(t *testing.T) {
	cases := []struct {
		pattern, key string
		want         bool
	}{
		{"woocommerce_uploads/**", "woocommerce_uploads/2026/07/a.pdf", true},
		{"woocommerce_uploads/**", "woocommerce_uploads", true},
		{"woocommerce_uploads/**", "other/2026/a.jpg", false},
		// A prefix that merely starts the same must not match: a bucket
		// with woocommerce_uploads_public/ next to it would otherwise
		// inherit the protection, or worse, lose it.
		{"woocommerce_uploads/**", "woocommerce_uploads_public/a.jpg", false},
		{"**", "anything/at/all", true},
		{"*.pdf", "manual.pdf", true},
		{"*.pdf", "2026/manual.pdf", false}, // a single star does not cross separators
		{"2026/*/a.jpg", "2026/07/a.jpg", true},
		{"2026/*/a.jpg", "2026/07/08/a.jpg", false},
	}
	for _, c := range cases {
		if got := MatchGlob(c.pattern, c.key); got != c.want {
			t.Errorf("MatchGlob(%q, %q) = %v, want %v", c.pattern, c.key, got, c.want)
		}
	}
}

func TestMatchAny(t *testing.T) {
	patterns := []string{"woocommerce_uploads/**", "*.sql"}
	if !MatchAny(patterns, "woocommerce_uploads/x.pdf") {
		t.Error("first pattern did not match")
	}
	if !MatchAny(patterns, "dump.sql") {
		t.Error("second pattern did not match")
	}
	if MatchAny(patterns, "2026/07/foto.jpg") {
		t.Error("an unrelated key matched")
	}
	if MatchAny(nil, "anything") {
		t.Error("an empty pattern list matched")
	}
}
