//go:build linux

package caps

import (
	"os"
	"os/exec"
)

func probe() Report {
	r := base()
	r.Root = os.Geteuid() == 0

	if release, err := os.ReadFile("/proc/sys/kernel/osrelease"); err == nil {
		r.Kernel = string(release)
		r.KernelMajor, r.KernelMinor = parseKernel(r.Kernel)
		r.Kernel = trimNewline(r.Kernel)
	}
	r.Passthrough = r.KernelAtLeast(passthroughMajor, passthroughMinor)

	if fi, err := os.Stat("/dev/fuse"); err == nil {
		r.DevFuse = fi.Mode()&os.ModeCharDevice != 0
	}
	// fusermount3 is what lets an unprivileged process unmount its own
	// mount; without it a non-root daemon can mount but not clean up.
	for _, name := range []string{"fusermount3", "fusermount"} {
		if p, err := exec.LookPath(name); err == nil {
			r.Fusermount = p
			break
		}
	}
	return r
}

func trimNewline(s string) string {
	for len(s) > 0 && (s[len(s)-1] == '\n' || s[len(s)-1] == '\r') {
		s = s[:len(s)-1]
	}
	return s
}
