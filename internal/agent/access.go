package agent

import (
	"context"
	"io"
	"io/fs"
	"path"
	"strings"
	"time"

	"github.com/ostap-mykhaylyak/tana/internal/index"
)

// The read-side surface the FUSE layer needs. It lives here rather
// than in internal/mount so that everything except the kernel binding
// itself can be tested without a mount.

// Entry returns an object's metadata from the index.
func (a *Agent) Entry(key string) (index.Entry, bool, error) {
	return a.idx.Get(a.ns(), key)
}

// Backing returns the directory holding the local bytes.
func (a *Agent) Backing() string { return a.site.Backing }

// Uploads returns the path WordPress knows.
func (a *Agent) Uploads() string { return a.site.Uploads }

// PathOf maps an object key to its path in the backing store.
func (a *Agent) PathOf(key string) string { return a.pathOf(key) }

// KeyOf maps a path in the backing store to an object key.
func (a *Agent) KeyOf(p string) (string, bool) { return a.keyOf(p) }

// Touch records that an object was read, which is what eviction
// orders by.
func (a *Agent) Touch(key string) { a.idx.Touch(a.ns(), key, time.Now()) }

// EvictedChildren returns the evicted objects directly inside dir,
// which have no directory entry of their own because their bytes are
// gone. A directory listing has to put them back, or WordPress cannot
// see files that very much still exist.
//
// dir is a key prefix, "" for the root of the site.
func (a *Agent) EvictedChildren(dir string) (map[string]index.Entry, error) {
	prefix := dir
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	out := map[string]index.Entry{}
	err := a.idx.WalkPrefix(a.ns(), prefix, func(e index.Entry) error {
		if e.State.Local() {
			return nil // it has a real directory entry already
		}
		rest := strings.TrimPrefix(e.Key, prefix)
		if name, _, nested := strings.Cut(rest, "/"); nested {
			// A subdirectory that exists only because something inside
			// it is evicted. Report it once, as a directory.
			if _, seen := out[name]; !seen {
				out[name] = index.Entry{Key: path.Join(prefix, name), Mode: uint32(fs.ModeDir | 0o750)}
			}
			return nil
		}
		out[rest] = e
		return nil
	})
	return out, err
}

// OpenRemote streams an object straight from the store without writing
// it to the backing store.
//
// This is what public traffic gets. A crawler walking an archive would
// otherwise recall every object it touches and undo the eviction that
// just made room; serving those reads through means the cache reflects
// what the site's own code uses, not what the internet asked for.
func (a *Agent) OpenRemote(ctx context.Context, key string) (io.ReadCloser, error) {
	rc, _, err := a.cli.Get(ctx, key)
	return rc, err
}

// ReadRemoteAt reads one range of an object straight from the store.
func (a *Agent) ReadRemoteAt(ctx context.Context, key string, p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	rc, err := a.cli.GetRange(ctx, key, off, int64(len(p)))
	if err != nil {
		return 0, err
	}
	defer rc.Close()
	n, err := io.ReadFull(rc, p)
	if err == io.ErrUnexpectedEOF || err == io.EOF {
		err = nil
	}
	return n, err
}
