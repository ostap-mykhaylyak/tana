// Package logging provides the independent JSON log streams of tana.
//
// Observability is reading log files: no dashboard, no rotation logic
// in the binary. Rotation is delegated to logrotate, which sends
// SIGHUP; the daemon then calls Reopen on every stream.
//
// The high-volume access stream is buffered: a write(2) per S3 request
// serialized on one mutex caps throughput well below what the store
// can otherwise serve. A hard crash may lose the last unflushed access
// lines, which is an acceptable trade for a log nobody bills from. The
// low-volume streams (service, transfer) stay unbuffered so a
// diagnostic line — or the last line before a crash — is on disk the
// instant it is written.
package logging

import (
	"bufio"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ostap-mykhaylyak/tana/internal/paths"
)

// flushInterval bounds how long a buffered line waits before hitting
// disk.
const flushInterval = 250 * time.Millisecond

// reopenFile is an *os.File that can be reopened in place (logrotate
// hook), optionally buffered. Writes are serialized with a mutex so a
// reopen never races a log line.
type reopenFile struct {
	mu   sync.Mutex
	path string
	f    *os.File
	bw   *bufio.Writer // nil when unbuffered
}

func openFile(path string, buffered bool) (*reopenFile, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		return nil, err
	}
	r := &reopenFile{path: path, f: f}
	if buffered {
		r.bw = bufio.NewWriterSize(f, 32*1024)
	}
	return r, nil
}

// dst returns the current write target; callers hold the lock.
func (r *reopenFile) dst() io.Writer {
	if r.bw != nil {
		return r.bw
	}
	return r.f
}

func (r *reopenFile) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.dst().Write(p)
}

// Flush drains the buffer to the file (no-op when unbuffered).
func (r *reopenFile) Flush() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.bw != nil {
		return r.bw.Flush()
	}
	return nil
}

func (r *reopenFile) Reopen() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.bw != nil {
		r.bw.Flush()
	}
	f, err := os.OpenFile(r.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		return err
	}
	old := r.f
	r.f = f
	if r.bw != nil {
		r.bw.Reset(f)
	}
	return old.Close()
}

func (r *reopenFile) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.bw != nil {
		r.bw.Flush()
	}
	return r.f.Close()
}

// Streams holds one independent *slog.Logger per concern.
type Streams struct {
	Service  *slog.Logger // daemon lifecycle, config reloads, errors
	Access   *slog.Logger // one line per S3 request served (buffered)
	Transfer *slog.Logger // uploads, recalls, evictions, journal shipping

	files []*reopenFile
	stop  chan struct{}
	once  sync.Once
}

// Open opens all log streams under dir (production: paths.LogDir).
func Open(dir string) (*Streams, error) {
	s := &Streams{stop: make(chan struct{})}
	for _, def := range []struct {
		name     string
		dst      **slog.Logger
		buffered bool
	}{
		{paths.ServiceLog, &s.Service, false},
		{paths.AccessLog, &s.Access, true},
		{paths.TransferLog, &s.Transfer, false},
	} {
		rf, err := openFile(filepath.Join(dir, def.name), def.buffered)
		if err != nil {
			s.Close()
			return nil, err
		}
		s.files = append(s.files, rf)
		*def.dst = slog.New(slog.NewJSONHandler(rf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	}
	go s.flushLoop()
	return s, nil
}

// flushLoop drains the buffered streams periodically until Close.
func (s *Streams) flushLoop() {
	t := time.NewTicker(flushInterval)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-t.C:
			for _, f := range s.files {
				f.Flush()
			}
		}
	}
}

// Reopen reopens every log file. Called on SIGHUP (logrotate hook).
func (s *Streams) Reopen() error {
	var errs []error
	for _, f := range s.files {
		if err := f.Reopen(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Close flushes and closes every log file. Called on shutdown.
func (s *Streams) Close() error {
	s.once.Do(func() { close(s.stop) })
	var errs []error
	for _, f := range s.files {
		if err := f.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
