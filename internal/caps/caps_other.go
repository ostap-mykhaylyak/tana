//go:build !linux

package caps

// probe on a non-Linux host reports only what is portable.
//
// tana is deployed exclusively on linux/amd64 and linux/arm64. This
// file is not a portability promise: it exists so the repository still
// builds and tests on a workstation, where the store role and the
// whole test suite run fine. The agent role never will — AgentReady
// says so, in those words, rather than failing later inside a mount.
func probe() Report { return base() }
