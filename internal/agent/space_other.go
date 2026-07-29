//go:build !linux

package agent

import "errors"

// freeSpace is unavailable off Linux, so the free-space floor simply
// does not apply there. tana is deployed on Linux only; this keeps the
// package building on a development workstation.
func freeSpace(string) (int64, error) { return 0, errors.ErrUnsupported }
