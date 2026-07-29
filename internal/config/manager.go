package config

import (
	"fmt"
	"path/filepath"
	"sync/atomic"

	"github.com/fsnotify/fsnotify"
)

// Manager holds the current configuration behind an atomic pointer:
// Get is cheap and safe to call on the hot path.
type Manager struct {
	path      string
	current   atomic.Pointer[Config]
	reloadErr atomic.Pointer[string]
}

// NewManager loads the config at path and returns a Manager for it.
func NewManager(path string) (*Manager, error) {
	cfg, err := Load(path)
	if err != nil {
		return nil, err
	}
	m := &Manager{path: path}
	m.current.Store(cfg)
	return m, nil
}

// Get returns the current configuration. Never nil.
func (m *Manager) Get() *Config { return m.current.Load() }

// Path returns the config file path this Manager watches.
func (m *Manager) Path() string { return m.path }

// LastError returns the error of the most recent failed reload, or ""
// if the last (re)load succeeded. The running config is never replaced
// by a broken one: on failure the previous config stays active.
func (m *Manager) LastError() string {
	if s := m.reloadErr.Load(); s != nil {
		return *s
	}
	return ""
}

// Reload re-parses the file and atomically swaps the pointer. On error
// the current config is left untouched and the error is retained for
// LastError.
func (m *Manager) Reload() error {
	cfg, err := Load(m.path)
	if err != nil {
		s := err.Error()
		m.reloadErr.Store(&s)
		return err
	}
	m.reloadErr.Store(nil)
	m.current.Store(cfg)
	return nil
}

// Watch reloads the config whenever the file changes on disk, until
// stop is closed. Callbacks run synchronously in the watch goroutine
// and must be fast.
//
// Note that not every field is hot-reloadable: listeners, FUSE mounts
// and the blob store root are bound at startup. Subsystems decide for
// themselves what they can pick up from a reloaded config.
func (m *Manager) Watch(stop <-chan struct{}, onErr func(error), onReload func(*Config)) error {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("config watch: %w", err)
	}
	// Watch the parent directory: editors replace the file via atomic
	// rename, which a file-level watch would lose track of.
	if err := w.Add(watchDir(m.path)); err != nil {
		w.Close()
		return fmt.Errorf("config watch: %w", err)
	}
	target := filepath.Clean(m.path)

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
				if filepath.Clean(ev.Name) != target {
					continue
				}
				if ev.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Rename) == 0 {
					continue
				}
				if err := m.Reload(); err != nil {
					if onErr != nil {
						onErr(err)
					}
				} else if onReload != nil {
					onReload(m.Get())
				}
			case err, ok := <-w.Errors:
				if !ok {
					return
				}
				if onErr != nil {
					onErr(err)
				}
			}
		}
	}()
	return nil
}

// watchDir returns the directory holding path, defaulting to "." so a
// bare filename in the working directory still watches something.
func watchDir(path string) string {
	dir := filepath.Dir(path)
	if dir == "" {
		return "."
	}
	return dir
}
