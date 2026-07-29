package caps

import "testing"

func TestParseKernel(t *testing.T) {
	cases := []struct {
		in           string
		major, minor int
	}{
		{"6.8.0-45-generic", 6, 8},
		{"6.14.2", 6, 14},
		{"5.15.0-91-generic\n", 5, 15},
		{"6.9.0+", 6, 9},
		{"", 0, 0},
		{"garbage", 0, 0},
		{"6", 0, 0},
	}
	for _, c := range cases {
		major, minor := parseKernel(c.in)
		if major != c.major || minor != c.minor {
			t.Errorf("parseKernel(%q) = %d.%d, want %d.%d", c.in, major, minor, c.major, c.minor)
		}
	}
}

func TestKernelAtLeast(t *testing.T) {
	cases := []struct {
		major, minor int
		want         bool
	}{
		{6, 9, true},   // exactly the passthrough floor
		{6, 14, true},  // newer minor
		{7, 0, true},   // newer major
		{6, 8, false},  // one minor short
		{5, 19, false}, // older major, higher minor
		{0, 0, false},  // unparseable release
	}
	for _, c := range cases {
		r := Report{KernelMajor: c.major, KernelMinor: c.minor}
		if got := r.KernelAtLeast(passthroughMajor, passthroughMinor); got != c.want {
			t.Errorf("kernel %d.%d: KernelAtLeast = %v, want %v", c.major, c.minor, got, c.want)
		}
	}
}

func TestAgentReadyNeedsLinux(t *testing.T) {
	r := Report{OS: "windows"}
	if err := r.AgentReady(); err == nil {
		t.Error("the agent role must refuse to run off Linux")
	}
	// The store is plain files and HTTP: it runs anywhere, which is
	// what makes developing on a workstation possible.
	if err := r.StoreReady(); err != nil {
		t.Errorf("StoreReady off Linux: %v", err)
	}
}

func TestAgentReadyNeedsDevFuse(t *testing.T) {
	r := Report{OS: "linux", Root: true}
	if err := r.AgentReady(); err == nil {
		t.Error("expected an error without /dev/fuse")
	}
	r.DevFuse = true
	if err := r.AgentReady(); err != nil {
		t.Errorf("root with /dev/fuse should be ready: %v", err)
	}
}

func TestAgentReadyUnprivilegedNeedsFusermount(t *testing.T) {
	r := Report{OS: "linux", DevFuse: true}
	if err := r.AgentReady(); err == nil {
		t.Error("a non-root agent without fusermount3 cannot unmount and must refuse")
	}
	r.Fusermount = "/usr/bin/fusermount3"
	if err := r.AgentReady(); err != nil {
		t.Errorf("non-root with fusermount3 should be ready: %v", err)
	}
}
