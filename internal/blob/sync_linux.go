//go:build linux

package blob

import "os"

// syncDir flushes a directory entry to stable storage.
//
// This is the step people leave out. Renaming a file into place is
// atomic, but the atomicity is about ordering, not durability: after a
// power cut the file can be intact while the directory entry naming it
// is gone, which is indistinguishable from never having written it.
func syncDir(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
