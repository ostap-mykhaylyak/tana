// Package service implements the bare service verbs (start, stop,
// restart, reload, enable, disable) as a thin wrapper over systemctl.
//
// They exist so an operator never has to remember whether tana is
// managed by systemd or run by hand: `tana start` is the answer either
// way. Everything that is not a service verb is a --flag, so the two
// can never be confused for one another.
package service

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strings"

	"github.com/ostap-mykhaylyak/tana/internal/paths"
)

// Verbs maps a tana verb to its systemctl equivalent.
var Verbs = map[string]string{
	"start":   "start",
	"stop":    "stop",
	"restart": "restart",
	"reload":  "reload",
	"enable":  "enable",
	"disable": "disable",
}

// IsVerb reports whether arg is a service verb.
func IsVerb(arg string) bool {
	_, ok := Verbs[arg]
	return ok
}

// order is the presentation order of the verbs in the usage text.
var order = []string{"start", "stop", "restart", "reload", "enable", "disable"}

// Names lists the verbs, for the usage text.
func Names() string { return strings.Join(order, "|") }

// Run executes a service verb. It returns an actionable error when
// systemd is not the thing managing this host, rather than a raw exit
// status nobody can read.
func Run(verb string, out io.Writer) error {
	sub, ok := Verbs[verb]
	if !ok {
		return fmt.Errorf("unknown command %q", verb)
	}
	if runtime.GOOS != "linux" {
		return fmt.Errorf("service verbs need systemd (this is %s): run the binary directly to start the daemon in the foreground", runtime.GOOS)
	}
	systemctl, err := exec.LookPath("systemctl")
	if err != nil {
		return fmt.Errorf("systemctl not found: run %s directly to start the daemon in the foreground", paths.Binary)
	}
	if _, err := os.Stat(paths.UnitFile); os.IsNotExist(err) {
		return fmt.Errorf("%s is not installed: run 'tana --init' first", paths.UnitFile)
	}
	if os.Geteuid() != 0 {
		return fmt.Errorf("tana %s requires root", verb)
	}

	cmd := exec.Command(systemctl, sub, paths.ServiceName)
	cmd.Stdout = out
	cmd.Stderr = out
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("systemctl %s %s: %w", sub, paths.ServiceName, err)
	}

	switch verb {
	case "start", "restart":
		fmt.Fprintln(out, "tana", verb+"ed;", "check it with: tana --status")
	case "reload":
		fmt.Fprintln(out, "tana reloaded (log files reopened)")
	case "enable":
		fmt.Fprintln(out, "tana will start at boot")
	case "disable":
		fmt.Fprintln(out, "tana will no longer start at boot")
	}
	return nil
}
