//go:build !linux

package blob

// syncDir is a no-op off Linux, where a directory cannot be opened for
// syncing. tana is deployed on Linux only; this exists so the package
// builds and its tests run on a development workstation, where nothing
// is expected to survive a power cut anyway.
func syncDir(string) error { return nil }
