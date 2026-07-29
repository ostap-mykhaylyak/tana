//go:build linux

package agent

import "golang.org/x/sys/unix"

// freeSpace reports the bytes available to an unprivileged process on
// the filesystem holding path.
//
// Bavail, not Bfree: the difference is the reserve only root may use,
// and tana's eviction must not plan to spend it.
func freeSpace(path string) (int64, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return 0, err
	}
	return int64(st.Bavail) * int64(st.Bsize), nil
}
