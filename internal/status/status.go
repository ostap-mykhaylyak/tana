// Package status is the control channel between the tana CLI and the
// running daemon: a Unix socket under /run, a one-line request, a JSON
// reply.
//
// It is deliberately not HTTP and not exposed on the network. Anything
// an operator needs to see about a running daemon is here; anything a
// client needs to do to the data goes through the S3 API instead.
package status

import (
	"time"

	"github.com/ostap-mykhaylyak/tana/internal/index"
)

// Commands understood by the socket.
const (
	CmdStatus = "status"
)

// Info is the full picture of a running daemon.
type Info struct {
	Version string    `json:"version"`
	PID     int       `json:"pid"`
	Started time.Time `json:"started"`
	Uptime  string    `json:"uptime"`

	Config      string   `json:"config"`
	ConfigError string   `json:"config_error,omitempty"`
	Warnings    []string `json:"warnings,omitempty"`

	Roles []string `json:"roles"`
	Caps  []string `json:"caps"`

	Store *StoreInfo `json:"store,omitempty"`
	Agent *AgentInfo `json:"agent,omitempty"`
}

// StoreInfo describes the S3 service side.
type StoreInfo struct {
	Listen  string `json:"listen"`
	Data    string `json:"data"`
	Replica string `json:"replica"`
	// JournalSeq is the last mutation written; AppliedSeq is how far
	// the index has folded them in. They are equal on a healthy store,
	// and their gap is the first thing to look at after a crash.
	JournalSeq  uint64      `json:"journal_seq"`
	AppliedSeq  uint64      `json:"applied_seq"`
	JournalNote string      `json:"journal_note,omitempty"`
	Buckets     []Namespace `json:"buckets"`
}

// AgentInfo describes the WordPress side.
type AgentInfo struct {
	Sites []Namespace `json:"sites"`
}

// Namespace is one site or one bucket, with its index counters.
type Namespace struct {
	Name  string      `json:"name"`
	Stats index.Stats `json:"stats"`
	// Note is set when the subsystem is present in config but not yet
	// running (a milestone that has not landed, or a failed mount).
	Note string `json:"note,omitempty"`
}

// Collector produces an Info on demand. The daemon supplies one; the
// socket server calls it per request.
type Collector func() Info
