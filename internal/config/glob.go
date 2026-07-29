package config

import (
	"path"
	"strings"
)

// MatchGlob matches an object key against a pattern, where a trailing
// /** crosses path separators and a bare * does not.
//
// path.Match alone is not enough: "woocommerce_uploads/**" has to
// match a key nested any number of directories deep, and that is the
// pattern every deployment will actually write. It lives here rather
// than in one of the packages that needs it because both the store's
// public-read policy and the agent's never_evict rules read the same
// patterns out of the same file, and two implementations of "what does
// this pattern cover" would eventually disagree.
func MatchGlob(pattern, key string) bool {
	if pattern == "**" {
		return true
	}
	// "dir/**" covers the directory itself and everything under it, at
	// any depth. Matching only the children would leave the directory
	// entry unprotected, which is a surprising thing for a protection
	// rule to do.
	if prefix, ok := strings.CutSuffix(pattern, "/**"); ok {
		return key == prefix || strings.HasPrefix(key, prefix+"/")
	}
	ok, err := path.Match(pattern, key)
	return err == nil && ok
}

// MatchAny reports whether any pattern matches the key.
func MatchAny(patterns []string, key string) bool {
	for _, p := range patterns {
		if MatchGlob(p, key) {
			return true
		}
	}
	return false
}
