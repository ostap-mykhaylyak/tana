// Package bootstrap provides the lifecycle operations of the bare
// binary: first-run auto-provisioning, the --init turnkey installer,
// the --purge destructive reset and the --keygen credential helper. It
// embeds the default filesystem skeleton (skel/) and the systemd unit.
package bootstrap

import (
	"bufio"
	"crypto/rand"
	"embed"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/ostap-mykhaylyak/tana/internal/paths"
)

//go:embed all:skel
var skelFS embed.FS

//go:embed tana.service
var UnitFile []byte

// skel source paths inside the embedded FS.
const (
	skelConfig    = "skel/etc/tana/config.yaml"
	skelLogrotate = "skel/etc/logrotate.d/tana"
)

// EnsureLayout creates the default filesystem layout and installs the
// default config WITHOUT overwriting an existing one. Used both by
// --init and by the first daemon start without a config.
func EnsureLayout(out io.Writer) error {
	for _, d := range []struct {
		path string
		perm fs.FileMode
	}{
		{paths.ConfigDir, 0o750},
		{paths.StateDir, 0o750},
		{paths.LogDir, 0o750},
	} {
		if err := os.MkdirAll(d.path, d.perm); err != nil {
			return fmt.Errorf("create %s: %w", d.path, err)
		}
	}
	created, err := installIfMissing(skelConfig, paths.ConfigFile, 0o640)
	if err != nil {
		return err
	}
	if created {
		fmt.Fprintf(out, "tana: installed default config at %s\n", paths.ConfigFile)
	}
	return nil
}

// installIfMissing copies an embedded skel file to dst unless dst
// already exists (operator files are never overwritten).
func installIfMissing(src, dst string, perm fs.FileMode) (bool, error) {
	if _, err := os.Stat(dst); err == nil {
		return false, nil
	}
	data, err := skelFS.ReadFile(src)
	if err != nil {
		return false, fmt.Errorf("embedded skel: %w", err)
	}
	if err := os.WriteFile(dst, data, perm); err != nil {
		return false, fmt.Errorf("install %s: %w", dst, err)
	}
	return true, nil
}

// Init is the turnkey installer behind --init: layout, binary in
// /sbin, systemd unit, logrotate policy. Lifecycle mode: it acts on
// the filesystem and does NOT assume a running service.
func Init(version string, out io.Writer) error {
	if err := requireRootLinux("--init"); err != nil {
		return err
	}
	if err := EnsureLayout(out); err != nil {
		return err
	}

	// Copy the running executable to /sbin/tana (unless it already is).
	self, err := os.Executable()
	if err != nil {
		return fmt.Errorf("locate executable: %w", err)
	}
	if self, err = filepath.EvalSymlinks(self); err != nil {
		return fmt.Errorf("locate executable: %w", err)
	}
	if self != paths.Binary {
		if err := copyFile(self, paths.Binary, 0o755); err != nil {
			return fmt.Errorf("install binary: %w", err)
		}
		fmt.Fprintf(out, "tana: installed binary at %s\n", paths.Binary)
	}

	if err := os.WriteFile(paths.UnitFile, UnitFile, 0o644); err != nil {
		return fmt.Errorf("install systemd unit: %w", err)
	}
	fmt.Fprintf(out, "tana: installed systemd unit at %s\n", paths.UnitFile)

	data, err := skelFS.ReadFile(skelLogrotate)
	if err != nil {
		return fmt.Errorf("embedded skel: %w", err)
	}
	if err := os.WriteFile(paths.LogrotateFile, data, 0o644); err != nil {
		return fmt.Errorf("install logrotate policy: %w", err)
	}
	fmt.Fprintf(out, "tana: installed logrotate policy at %s\n", paths.LogrotateFile)

	fmt.Fprintf(out, `
tana %s installed. It will not start until you tell it what to be:

  1. edit %s and set roles to [store], [agent] or both
  2. generate credentials with: tana --keygen
  3. tana --check-config
  4. systemctl daemon-reload
  5. tana start
  6. tana --status
`, version, paths.ConfigFile)
	return nil
}

// PurgeTargets returns, in one place, everything the app creates at
// runtime. The purge stays automatically aligned with the layout.
//
// The blob store (store.data) is deliberately NOT included: it is
// operator-chosen, it holds the only copy of the media, and a flag
// named --purge should not be able to delete a terabyte of customer
// uploads because it once appeared in a config file.
func PurgeTargets() []string {
	return []string{paths.ConfigDir, paths.StateDir, paths.LogDir, paths.RunDir}
}

// allowedPurgePrefixes guards against a misconfigured paths package in
// a custom build: purge refuses to touch anything outside these.
var allowedPurgePrefixes = []string{"/etc/tana", "/var/lib/tana", "/var/log/tana", "/run/tana"}

// Purge is the destructive reset behind --purge: removes config, index
// and logs, returning the host to "never installed". It is NOT
// uninstall (binary and systemd unit are left in place) and it does
// not touch the blobs.
func Purge(assumeYes bool, in io.Reader, out io.Writer) error {
	if err := requireRootLinux("--purge"); err != nil {
		return err
	}

	// Never delete state under a live process.
	if err := exec.Command("systemctl", "is-active", "--quiet", paths.ServiceName).Run(); err == nil {
		return fmt.Errorf("service is running: stop it first (tana stop)")
	}

	targets := PurgeTargets()
	for _, t := range targets {
		if !purgeAllowed(t) {
			return fmt.Errorf("refusing to remove unexpected path %q", t)
		}
	}

	fmt.Fprintln(out, "The following paths and ALL their contents will be removed:")
	for _, t := range targets {
		fmt.Fprintln(out, "  ", t)
	}
	fmt.Fprintln(out, "\nThe blob store is NOT touched: remove it by hand if you mean to.")
	fmt.Fprintln(out, "Removing the index without removing the blobs is recoverable (tana --fsck rebuilds it).")
	if !assumeYes {
		if !stdinIsTerminal(in) {
			return fmt.Errorf("refusing to purge without --yes (stdin is not a terminal)")
		}
		fmt.Fprint(out, "Type 'yes' to confirm: ")
		line, _ := bufio.NewReader(in).ReadString('\n')
		if strings.TrimSpace(line) != "yes" {
			return fmt.Errorf("aborted")
		}
	}

	var errs []string
	removed := 0
	for _, t := range targets {
		if _, err := os.Stat(t); os.IsNotExist(err) {
			continue
		}
		if err := os.RemoveAll(t); err != nil {
			errs = append(errs, err.Error())
			continue
		}
		fmt.Fprintln(out, "removed", t)
		removed++
	}
	fmt.Fprintf(out, "removed %d path(s)\n", removed)
	fmt.Fprintln(out, "run 'tana --init' to provision from scratch")
	if len(errs) > 0 {
		return fmt.Errorf("some paths could not be removed: %s", strings.Join(errs, "; "))
	}
	return nil
}

// Keygen prints a fresh access key / secret key pair, ready to paste
// into both the store's bucket entry and the agent's backend block.
func Keygen(out io.Writer) error {
	access, err := randomString(20, "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567")
	if err != nil {
		return err
	}
	secret, err := randomString(40, "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/")
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "access_key: \"TANA%s\"\nsecret_key: \"%s\"\n", access, secret)
	return nil
}

// randomString draws n characters from alphabet using crypto/rand,
// rejecting values that would bias the modulo.
func randomString(n int, alphabet string) (string, error) {
	// Largest multiple of len(alphabet) that fits in a byte; drawing
	// above it and taking the modulo would favour the early letters.
	limit := 256 - (256 % len(alphabet))
	out := make([]byte, 0, n)
	buf := make([]byte, n)
	for len(out) < n {
		if _, err := rand.Read(buf); err != nil {
			return "", fmt.Errorf("keygen: %w", err)
		}
		for _, b := range buf {
			if int(b) >= limit {
				continue
			}
			out = append(out, alphabet[int(b)%len(alphabet)])
			if len(out) == n {
				break
			}
		}
	}
	return string(out), nil
}

func purgeAllowed(path string) bool {
	if path == "" || path == "/" {
		return false
	}
	for _, p := range allowedPurgePrefixes {
		if path == p || strings.HasPrefix(path, p+"/") {
			return true
		}
	}
	return false
}

func requireRootLinux(op string) error {
	if runtime.GOOS != "linux" {
		return fmt.Errorf("%s only runs on Linux", op)
	}
	if os.Geteuid() != 0 {
		return fmt.Errorf("%s requires root", op)
	}
	return nil
}

func stdinIsTerminal(in io.Reader) bool {
	f, ok := in.(*os.File)
	if !ok {
		return false
	}
	fi, err := f.Stat()
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeCharDevice != 0
}

func copyFile(src, dst string, perm fs.FileMode) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	// Write to a temp file in the same dir and rename: atomic, and it
	// works even while the destination is being executed.
	tmp := dst + ".tmp"
	if err := os.WriteFile(tmp, data, perm); err != nil {
		return err
	}
	return os.Rename(tmp, dst)
}
