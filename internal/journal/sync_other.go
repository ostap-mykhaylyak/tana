//go:build !linux

package journal

// syncDir is a no-op off Linux, where a directory cannot be opened for
// syncing. See internal/blob/sync_other.go for why that is acceptable.
func syncDir(string) error { return nil }
