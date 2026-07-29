// Package agent is the WordPress side: it keeps a site's uploads
// directory and the store in step.
//
// The durable state of the queue is the index, not a list in memory.
// Every object is in one of the states internal/index defines, and
// "dirty" means the bytes exist locally and nowhere else. A restart
// therefore does not lose queued work: it re-derives the queue by
// asking the index which objects are still dirty. An in-memory queue
// that had to be flushed cleanly on shutdown would be a queue that
// silently loses uploads whenever the machine does not shut down
// cleanly, which is exactly when it matters.
package agent

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"

	"github.com/ostap-mykhaylyak/tana/internal/config"
	"github.com/ostap-mykhaylyak/tana/internal/index"
	"github.com/ostap-mykhaylyak/tana/internal/s3client"
)

const (
	// workers is how many uploads run at once. Media files are small
	// and the store is on the LAN, so the useful limit is the store's
	// fsync rate, not bandwidth.
	workers = 4
	// queueDepth bounds the in-memory hand-off. Overflow is not a
	// problem: anything that does not fit stays dirty in the index and
	// the next sweep picks it up.
	queueDepth = 4096
	// sweepInterval is how often the index is re-checked for work the
	// live queue missed.
	sweepInterval = 2 * time.Minute
	// maxAttempts bounds one object's retry loop before it is left for
	// the next sweep.
	maxAttempts = 5
	// baseBackoff is the first retry delay; it doubles per attempt.
	baseBackoff = 500 * time.Millisecond
)

// Agent keeps one site in step with its bucket.
type Agent struct {
	site    config.Site
	idx     *index.DB
	cli     *s3client.Client
	svcLog  *slog.Logger
	xferLog *slog.Logger

	queue chan string
	wg    sync.WaitGroup

	mu      sync.Mutex
	pending map[string]bool // keys already queued, to avoid duplicates

	// recalling collapses concurrent recalls of the same key.
	recalling recalls
}

// New builds an agent for one site.
func New(site config.Site, idx *index.DB, svcLog, xferLog *slog.Logger) (*Agent, error) {
	cli, err := s3client.New(site.Backend, 0)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(site.Backing, 0o750); err != nil {
		return nil, fmt.Errorf("agent: create backing store %s: %w", site.Backing, err)
	}
	return &Agent{
		site:    site,
		idx:     idx,
		cli:     cli,
		svcLog:  svcLog,
		xferLog: xferLog,
		queue:   make(chan string, queueDepth),
		pending: map[string]bool{},
	}, nil
}

// Name returns the site name, which is also its index namespace.
func (a *Agent) Name() string { return a.site.Name }

// Site returns the configuration this agent was built from.
func (a *Agent) Site() config.Site { return a.site }

// Client exposes the S3 client, for recall (M4) and the scrub.
func (a *Agent) Client() *s3client.Client { return a.cli }

// Start launches the workers, the directory watcher and the sweeper.
func (a *Agent) Start(stop <-chan struct{}) error {
	for i := 0; i < workers; i++ {
		a.wg.Add(1)
		go a.worker(stop)
	}

	// An initial scan before anything else: whatever changed while the
	// daemon was down is found here, not by waiting for a file event
	// that already happened.
	if _, err := a.Scan(); err != nil {
		return err
	}
	if err := a.watch(stop); err != nil {
		return err
	}
	go a.sweep(stop)
	return nil
}

// Wait blocks until the workers have stopped.
func (a *Agent) Wait() { a.wg.Wait() }

// ScanStats is what one reconciliation pass found.
type ScanStats struct {
	Files   int
	New     int
	Changed int
	Removed int
}

// Scan reconciles the backing store against the index.
//
// It is the authority on what exists: a file present on disk and
// absent from the index is a new upload, and an index entry whose file
// is gone is a deletion. WordPress writes to the uploads directory
// with ordinary file operations, so this is the only way to learn what
// it did.
func (a *Agent) Scan() (ScanStats, error) {
	var st ScanStats
	seen := make(map[string]bool)

	err := filepath.WalkDir(a.site.Backing, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		key, ok := a.keyOf(p)
		if !ok {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		st.Files++
		seen[key] = true

		prev, exists, err := a.idx.Get(a.site.Name, key)
		if err != nil {
			return err
		}
		switch {
		case !exists:
			st.New++
		case prev.Size != info.Size() || !prev.ModTime.Equal(info.ModTime()):
			st.Changed++
		default:
			return nil // unchanged and already known
		}

		if err := a.idx.Put(a.site.Name, index.Entry{
			Key:     key,
			Size:    info.Size(),
			ModTime: info.ModTime(),
			Mode:    uint32(info.Mode().Perm()),
			State:   index.Dirty,
			ATime:   time.Now(),
		}); err != nil {
			return err
		}
		a.Enqueue(key)
		return nil
	})
	if err != nil {
		return st, fmt.Errorf("agent: scan %s: %w", a.site.Backing, err)
	}

	// Entries whose file is gone. An evicted object has no local file
	// by design, so it is not a deletion and must be left alone.
	var gone []index.Entry
	if err := a.idx.Walk(a.site.Name, func(e index.Entry) error {
		if !seen[e.Key] && e.State.Local() {
			gone = append(gone, e)
		}
		return nil
	}); err != nil {
		return st, err
	}
	for _, e := range gone {
		if err := a.remove(e.Key); err != nil {
			a.svcLog.Error("could not propagate a deletion", "site", a.site.Name, "key", e.Key, "error", err)
			continue
		}
		st.Removed++
	}
	return st, nil
}

// Enqueue offers a key to the workers. A full queue is not an error:
// the object stays dirty and the next sweep finds it.
func (a *Agent) Enqueue(key string) {
	a.mu.Lock()
	if a.pending[key] {
		a.mu.Unlock()
		return
	}
	a.pending[key] = true
	a.mu.Unlock()

	select {
	case a.queue <- key:
	default:
		a.mu.Lock()
		delete(a.pending, key)
		a.mu.Unlock()
	}
}

// worker uploads queued objects until stop is closed.
func (a *Agent) worker(stop <-chan struct{}) {
	defer a.wg.Done()
	for {
		select {
		case <-stop:
			return
		case key := <-a.queue:
			a.upload(stop, key)
			a.mu.Lock()
			delete(a.pending, key)
			a.mu.Unlock()
		}
	}
}

// upload sends one object, retrying transient failures.
func (a *Agent) upload(stop <-chan struct{}, key string) {
	e, ok, err := a.idx.Get(a.site.Name, key)
	if err != nil || !ok || e.State.Safe() {
		return // already uploaded, or gone
	}

	path := a.pathOf(key)
	backoff := baseBackoff
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			select {
			case <-stop:
				cancel()
			case <-ctx.Done():
			}
		}()
		etag, err := a.cli.PutFile(ctx, key, path)
		cancel()

		if err == nil {
			if _, err := a.idx.SetState(a.site.Name, key, index.Synced); err != nil {
				a.svcLog.Error("upload succeeded but the index did not record it",
					"site", a.site.Name, "key", key, "error", err)
				return
			}
			a.xferLog.Info("uploaded", "site", a.site.Name, "key", key,
				"size", e.Size, "etag", etag, "attempts", attempt)
			return
		}

		// A file that vanished mid-upload is not a failure: WordPress
		// deleted it, and the next scan will propagate that.
		if errors.Is(err, fs.ErrNotExist) {
			return
		}
		var apiErr *s3client.Error
		if errors.As(err, &apiErr) && !apiErr.Retryable() {
			a.svcLog.Error("upload refused by the store",
				"site", a.site.Name, "key", key, "error", err)
			return
		}
		select {
		case <-stop:
			return
		case <-time.After(backoff):
		}
		backoff *= 2
	}
	// Out of attempts. The object is still dirty in the index, so the
	// sweep will try again rather than the upload being lost.
	a.xferLog.Warn("upload still pending after retries",
		"site", a.site.Name, "key", key, "attempts", maxAttempts)
}

// remove propagates a local deletion to the store.
func (a *Agent) remove(key string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := a.cli.Delete(ctx, key); err != nil {
		return err
	}
	if err := a.idx.Delete(a.site.Name, key); err != nil {
		return err
	}
	a.xferLog.Info("deleted", "site", a.site.Name, "key", key)
	return nil
}

// sweep re-enqueues anything still dirty, on an interval.
//
// The live queue can drop work when it is full and a worker can give
// up after its retries; neither loses data, because the index still
// says the object is dirty. This is what turns that fact into an
// upload actually happening.
func (a *Agent) sweep(stop <-chan struct{}) {
	t := time.NewTicker(sweepInterval)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			var dirty []string
			if err := a.idx.Walk(a.site.Name, func(e index.Entry) error {
				if !e.State.Safe() {
					dirty = append(dirty, e.Key)
				}
				return nil
			}); err != nil {
				a.svcLog.Error("sweep failed", "site", a.site.Name, "error", err)
				continue
			}
			for _, key := range dirty {
				a.Enqueue(key)
			}
			if len(dirty) > 0 {
				a.xferLog.Info("sweep re-queued pending uploads",
					"site", a.site.Name, "count", len(dirty))
			}
		}
	}
}

// watch follows the backing store and queues what changes.
func (a *Agent) watch(stop <-chan struct{}) error {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("agent: watch: %w", err)
	}
	// Watch every directory: fsnotify is not recursive, and WordPress
	// creates a new one every month.
	if err := filepath.WalkDir(a.site.Backing, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return w.Add(p)
		}
		return nil
	}); err != nil {
		w.Close()
		return fmt.Errorf("agent: watch: %w", err)
	}

	go func() {
		defer w.Close()
		for {
			select {
			case <-stop:
				return
			case ev, ok := <-w.Events:
				if !ok {
					return
				}
				a.handleEvent(w, ev)
			case err, ok := <-w.Errors:
				if !ok {
					return
				}
				a.svcLog.Error("watch error", "site", a.site.Name, "error", err)
			}
		}
	}()
	return nil
}

// handleEvent turns one filesystem event into queued work.
func (a *Agent) handleEvent(w *fsnotify.Watcher, ev fsnotify.Event) {
	key, ok := a.keyOf(ev.Name)
	if !ok {
		return
	}
	if ev.Op&(fsnotify.Remove|fsnotify.Rename) != 0 {
		// Removals are handled by the scan rather than here: a rename
		// arrives as a remove followed by a create, and acting on the
		// remove alone would delete an object that still exists under
		// its new name.
		a.Enqueue(key)
		return
	}
	fi, err := os.Stat(ev.Name)
	if err != nil {
		return
	}
	if fi.IsDir() {
		// A new month directory. Watch it, and scan it: files may have
		// landed between its creation and this line.
		w.Add(ev.Name)
		if _, err := a.Scan(); err != nil {
			a.svcLog.Error("scan after new directory failed", "site", a.site.Name, "error", err)
		}
		return
	}
	if ev.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Chmod) == 0 {
		return
	}
	if err := a.idx.Put(a.site.Name, index.Entry{
		Key:     key,
		Size:    fi.Size(),
		ModTime: fi.ModTime(),
		Mode:    uint32(fi.Mode().Perm()),
		State:   index.Dirty,
		ATime:   time.Now(),
	}); err != nil {
		a.svcLog.Error("could not index a changed file", "site", a.site.Name, "key", key, "error", err)
		return
	}
	a.Enqueue(key)
}

// keyOf maps a path in the backing store to an object key. Keys always
// use forward slashes: they are S3 keys, not paths.
func (a *Agent) keyOf(p string) (string, bool) {
	rel, err := filepath.Rel(a.site.Backing, p)
	if err != nil || rel == "." || strings.HasPrefix(rel, "..") {
		return "", false
	}
	return filepath.ToSlash(rel), true
}

// pathOf maps an object key back to its path in the backing store.
func (a *Agent) pathOf(key string) string {
	return filepath.Join(a.site.Backing, filepath.FromSlash(key))
}

// Stats returns the site's index counters.
func (a *Agent) Stats() (index.Stats, error) { return a.idx.Stats(a.site.Name) }

// Drain blocks until the queue is empty and no upload is in flight, or
// the deadline passes. It exists for tests and for --sync; the running
// daemon never needs to wait.
func (a *Agent) Drain(ctx context.Context) error {
	for {
		a.mu.Lock()
		inflight := len(a.pending)
		a.mu.Unlock()
		if inflight == 0 && len(a.queue) == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}
