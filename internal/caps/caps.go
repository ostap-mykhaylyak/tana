// Package caps probes what the host can actually do, so the daemon
// fails at startup with a sentence the operator can act on instead of
// somewhere deep inside a mount call.
//
// The agent role has hard requirements (Linux, /dev/fuse, privileges)
// and one soft one: FUSE passthrough, added in Linux 6.9, lets the
// kernel serve reads and writes of cached files without a round trip
// through userspace. Without it tana still works, it just pays a
// context switch per I/O on files it is only passing through.
package caps

import (
	"fmt"
	"runtime"
	"strconv"
	"strings"
)

// passthroughMajor/Minor is the first kernel with FUSE passthrough.
const (
	passthroughMajor = 6
	passthroughMinor = 9
)

// Report is what the host offers.
type Report struct {
	OS   string
	Arch string
	// Kernel is the raw release string ("6.8.0-45-generic"), empty off
	// Linux.
	Kernel string
	// KernelMajor/Minor are parsed from Kernel; zero when unparseable.
	KernelMajor int
	KernelMinor int
	// Root reports whether the process has euid 0.
	Root bool
	// DevFuse reports whether /dev/fuse exists and is usable.
	DevFuse bool
	// Fusermount is the path to fusermount3, empty when not installed.
	// Needed to unmount cleanly as a non-root user.
	Fusermount string
	// Passthrough reports whether the kernel supports FUSE passthrough.
	Passthrough bool
}

// Probe inspects the host.
func Probe() Report { return probe() }

// AgentReady returns nil when the agent role can mount, or the reason
// it cannot.
func (r Report) AgentReady() error {
	if r.OS != "linux" {
		return fmt.Errorf("the agent role requires Linux (this is %s): the store role runs anywhere", r.OS)
	}
	if !r.DevFuse {
		return fmt.Errorf("/dev/fuse is missing: load the fuse module (modprobe fuse) or enable it for this container")
	}
	if !r.Root && r.Fusermount == "" {
		return fmt.Errorf("not running as root and fusermount3 is not installed: install fuse3")
	}
	return nil
}

// StoreReady returns nil when the store role can run. The store is
// plain files and HTTP, so it has no kernel requirements.
func (r Report) StoreReady() error { return nil }

// KernelAtLeast reports whether the running kernel is at least
// major.minor.
func (r Report) KernelAtLeast(major, minor int) bool {
	if r.KernelMajor != major {
		return r.KernelMajor > major
	}
	return r.KernelMinor >= minor
}

// Lines renders the report for --caps, one "key: value" per line.
func (r Report) Lines() []string {
	kernel := r.Kernel
	if kernel == "" {
		kernel = "n/a"
	}
	fusermount := r.Fusermount
	if fusermount == "" {
		fusermount = "not found"
	}
	return []string{
		fmt.Sprintf("os:          %s/%s", r.OS, r.Arch),
		fmt.Sprintf("kernel:      %s", kernel),
		fmt.Sprintf("root:        %t", r.Root),
		fmt.Sprintf("/dev/fuse:   %t", r.DevFuse),
		fmt.Sprintf("fusermount3: %s", fusermount),
		fmt.Sprintf("passthrough: %t (needs linux >= %d.%d)", r.Passthrough, passthroughMajor, passthroughMinor),
	}
}

// base fills in what is knowable without touching the host.
func base() Report {
	return Report{OS: runtime.GOOS, Arch: runtime.GOARCH}
}

// parseKernel extracts major and minor from a release string such as
// "6.8.0-45-generic". Returns zeros when the shape is unexpected.
func parseKernel(release string) (major, minor int) {
	rel := strings.TrimSpace(release)
	if i := strings.IndexAny(rel, "-+"); i >= 0 {
		rel = rel[:i]
	}
	parts := strings.Split(rel, ".")
	if len(parts) < 2 {
		return 0, 0
	}
	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0
	}
	minor, err = strconv.Atoi(parts[1])
	if err != nil {
		return major, 0
	}
	return major, minor
}
