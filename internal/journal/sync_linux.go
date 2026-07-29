//go:build linux

package journal

import "os"

// syncDir flushes a directory entry to stable storage, so a freshly
// created segment file survives a power cut along with its records.
func syncDir(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
