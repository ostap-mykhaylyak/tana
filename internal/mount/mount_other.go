//go:build !linux

// Package mount presents a site's backing store at the path WordPress
// knows. It is implemented only on Linux, which is tana's only
// deployment target; this file exists so the repository builds on a
// development workstation.
package mount

import (
	"fmt"
	"log/slog"
	"runtime"

	"github.com/ostap-mykhaylyak/tana/internal/agent"
)

// Options tunes a mount.
type Options struct {
	PopulateUID uint32
	AllowOther  bool
	Debug       bool
}

// Mount is one site's live filesystem.
type Mount struct{}

// New always fails off Linux. The daemon refuses the agent role well
// before reaching here (see internal/caps), so this is a backstop
// rather than a path anything takes.
func New(*agent.Agent, Options, *slog.Logger) (*Mount, error) {
	return nil, fmt.Errorf("mount: FUSE is available only on Linux (this is %s)", runtime.GOOS)
}

// Wait is a no-op.
func (*Mount) Wait() {}

// Unmount is a no-op.
func (*Mount) Unmount() error { return nil }
