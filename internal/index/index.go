// Package index is the local object index: the metadata plane.
//
// It exists because of the one measurement that shapes all of tana:
// WordPress performs hundreds of metadata operations (stat, exists,
// filesize, readdir) per request and only a handful of data
// operations. Anything that puts stat on the network is finished
// before it starts. So every question about an object that is not
// "give me the bytes" is answered from here, in-process, from an
// mmap'd bbolt file.
//
// Layout: one bbolt bucket of objects per namespace (a site name on
// the agent, a bucket name on the store), keyed by the object key
// itself. bbolt keeps keys sorted, which gives prefix scans — and
// therefore ListObjectsV2 with prefix and delimiter — for free.
//
// Access times are handled apart from everything else. They change on
// every read, and a bbolt transaction per read would mean an fsync per
// read. They are instead accumulated in memory and flushed in a single
// transaction periodically: losing the last few seconds of access
// times costs nothing but eviction accuracy.
package index

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

// schemaVersion guards against opening an index written by a future
// build with an incompatible layout.
const schemaVersion = 1

// atimeFlushInterval bounds how long an access time stays in memory.
const atimeFlushInterval = 30 * time.Second

// Bucket names. Object buckets are namespaced with a prefix so one
// file can hold every site on the machine.
const (
	metaBucket   = "meta"
	objPrefix    = "obj:"
	statsBucket  = "stats"
	versionKey   = "schema"
	openFileMode = 0o640
)

// State is where an object's bytes currently are.
type State uint8

const (
	// Dirty: bytes exist only in the local backing store. The object
	// is not safe yet and must never be evicted.
	Dirty State = iota
	// Uploading: a writeback worker has picked it up.
	Uploading
	// Synced: bytes exist both locally and on the store.
	Synced
	// Evicted: bytes exist only on the store. Metadata below is still
	// authoritative, which is what lets stat stay local.
	Evicted
)

var stateNames = map[State]string{
	Dirty: "dirty", Uploading: "uploading", Synced: "synced", Evicted: "evicted",
}

// String implements fmt.Stringer.
func (s State) String() string {
	if n, ok := stateNames[s]; ok {
		return n
	}
	return fmt.Sprintf("state(%d)", uint8(s))
}

// ParseState maps a name back to its State.
func ParseState(name string) (State, bool) {
	for s, n := range stateNames {
		if n == name {
			return s, true
		}
	}
	return 0, false
}

// Local reports whether the bytes are on local disk.
func (s State) Local() bool { return s != Evicted }

// Safe reports whether the bytes are known to exist on the store.
func (s State) Safe() bool { return s == Synced || s == Evicted }

// Entry is everything tana knows about one object without touching
// its bytes. The fields WordPress can observe (Size, ModTime, Mode)
// stay accurate in every state, evicted included.
type Entry struct {
	Key     string    `json:"key"`
	Size    int64     `json:"size"`
	ModTime time.Time `json:"mtime"`
	Mode    uint32    `json:"mode"`
	// Hash is the hex sha256 of the content: the blob's address on the
	// store, and the checksum the scrub verifies against.
	Hash string `json:"hash"`
	// ETag is the S3 entity tag — the md5 for a whole-object upload,
	// or "<md5 of the part md5s>-<count>" for a multipart one. It is
	// kept only because clients compare it; nothing in tana relies on
	// it, and nothing should: md5 is not a checksum tana would choose.
	ETag   string    `json:"etag,omitempty"`
	State  State     `json:"state"`
	ATime  time.Time `json:"atime"`
	Pinned bool      `json:"pinned"`
}

// Stats is the aggregate view of one namespace, maintained
// transactionally so eviction never has to walk the whole index to
// learn how much disk it is using.
type Stats struct {
	Objects      int64 `json:"objects"`
	Bytes        int64 `json:"bytes"`
	LocalObjects int64 `json:"local_objects"`
	LocalBytes   int64 `json:"local_bytes"`
	DirtyObjects int64 `json:"dirty_objects"`
	DirtyBytes   int64 `json:"dirty_bytes"`
}

// DB is the index. Safe for concurrent use.
type DB struct {
	b *bolt.DB

	mu      sync.Mutex
	pending map[string]map[string]time.Time // ns -> key -> atime
	stop    chan struct{}
	once    sync.Once
}

// Open opens (creating it if needed) the index at path.
func Open(path string) (*DB, error) {
	b, err := bolt.Open(path, openFileMode, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		if hint := portabilityHint(path, err); hint != "" {
			return nil, fmt.Errorf("open index %s: %w\n%s", path, err, hint)
		}
		return nil, fmt.Errorf("open index %s: %w", path, err)
	}
	db := &DB{b: b, pending: make(map[string]map[string]time.Time), stop: make(chan struct{})}
	if err := db.initSchema(); err != nil {
		b.Close()
		return nil, err
	}
	go db.atimeLoop()
	return db, nil
}

// portabilityHint explains the one way a healthy-looking index file
// refuses to open: bbolt writes its pages at the page size of the host
// that created the file, and arm64 is not uniformly 4K — 16K and 64K
// kernels are both shipped. Copying an index between a 4K and a 64K
// host produces a bare "invalid database" that looks like corruption
// and is not. The blobs are untouched in that situation, so the fix is
// to rebuild rather than to restore.
func portabilityHint(path string, err error) string {
	if !errors.Is(err, bolt.ErrInvalid) {
		return ""
	}
	if fi, statErr := os.Stat(path); statErr != nil || fi.Size() == 0 {
		return ""
	}
	return "the index is written at the page size of the machine that created it, and page size " +
		"differs across arm64 kernels (4K, 16K, 64K). An index copied from a host with a different " +
		"page size cannot be opened: delete it and rebuild with 'tana --fsck'. The blobs are unaffected."
}

// initSchema stamps the schema version on a fresh file and refuses to
// open one written by a newer build.
func (d *DB) initSchema() error {
	return d.b.Update(func(tx *bolt.Tx) error {
		mb, err := tx.CreateBucketIfNotExists([]byte(metaBucket))
		if err != nil {
			return err
		}
		got := mb.Get([]byte(versionKey))
		if got == nil {
			return mb.Put([]byte(versionKey), []byte(fmt.Sprint(schemaVersion)))
		}
		var v int
		if _, err := fmt.Sscanf(string(got), "%d", &v); err != nil {
			return fmt.Errorf("index: unreadable schema version %q", got)
		}
		if v > schemaVersion {
			return fmt.Errorf("index: schema version %d is newer than this build supports (%d)", v, schemaVersion)
		}
		return nil
	})
}

// Close flushes pending access times and closes the file.
func (d *DB) Close() error {
	d.once.Do(func() { close(d.stop) })
	d.flushATimes()
	return d.b.Close()
}

// Put inserts or replaces an entry, keeping the namespace stats in the
// same transaction so they can never drift from the objects.
func (d *DB) Put(ns string, e Entry) error {
	return d.Update(func(t *Tx) error { return t.Put(ns, e) })
}

// Get returns the entry for key, merging any access time still pending
// in memory so callers never see a stale ATime.
func (d *DB) Get(ns, key string) (Entry, bool, error) {
	var e Entry
	var found bool
	err := d.b.View(func(tx *bolt.Tx) error {
		ob := tx.Bucket(objKey(ns))
		if ob == nil {
			return nil
		}
		raw := ob.Get([]byte(key))
		if raw == nil {
			return nil
		}
		found = true
		return json.Unmarshal(raw, &e)
	})
	if err != nil || !found {
		return Entry{}, found, err
	}
	d.mu.Lock()
	if at, ok := d.pending[ns][key]; ok && at.After(e.ATime) {
		e.ATime = at
	}
	d.mu.Unlock()
	return e, true, nil
}

// Delete removes an entry. Deleting a missing key is not an error:
// callers reconciling against a filesystem should not have to check
// first.
func (d *DB) Delete(ns, key string) error {
	d.mu.Lock()
	delete(d.pending[ns], key)
	d.mu.Unlock()

	return d.Update(func(t *Tx) error {
		_, _, err := t.Delete(ns, key)
		return err
	})
}

// SetState transitions an object without rewriting the rest of its
// metadata. Returns false when the key is unknown.
func (d *DB) SetState(ns, key string, s State) (bool, error) {
	var ok bool
	err := d.Update(func(t *Tx) error {
		var err error
		ok, err = t.SetState(ns, key, s)
		return err
	})
	return ok, err
}

// Touch records an access time in memory. Cheap enough to call on
// every read; the flusher persists it within atimeFlushInterval.
func (d *DB) Touch(ns, key string, at time.Time) {
	d.mu.Lock()
	m := d.pending[ns]
	if m == nil {
		m = make(map[string]time.Time)
		d.pending[ns] = m
	}
	if cur, ok := m[key]; !ok || at.After(cur) {
		m[key] = at
	}
	d.mu.Unlock()
}

// Walk calls fn for every entry in the namespace, in key order. fn
// must not call back into the DB: it runs inside a read transaction.
// Returning a non-nil error stops the walk and propagates.
func (d *DB) Walk(ns string, fn func(Entry) error) error {
	return d.b.View(func(tx *bolt.Tx) error {
		ob := tx.Bucket(objKey(ns))
		if ob == nil {
			return nil
		}
		return ob.ForEach(func(k, raw []byte) error {
			var e Entry
			if err := json.Unmarshal(raw, &e); err != nil {
				return fmt.Errorf("index: corrupt entry %s/%s: %w", ns, k, err)
			}
			return fn(e)
		})
	})
}

// WalkPrefix is Walk restricted to keys starting with prefix, using
// bbolt's ordered cursor rather than filtering a full scan.
func (d *DB) WalkPrefix(ns, prefix string, fn func(Entry) error) error {
	return d.b.View(func(tx *bolt.Tx) error {
		ob := tx.Bucket(objKey(ns))
		if ob == nil {
			return nil
		}
		c := ob.Cursor()
		p := []byte(prefix)
		for k, raw := c.Seek(p); k != nil && strings.HasPrefix(string(k), prefix); k, raw = c.Next() {
			var e Entry
			if err := json.Unmarshal(raw, &e); err != nil {
				return fmt.Errorf("index: corrupt entry %s/%s: %w", ns, k, err)
			}
			if err := fn(e); err != nil {
				return err
			}
		}
		return nil
	})
}

// Stats returns the aggregate counters for a namespace.
func (d *DB) Stats(ns string) (Stats, error) {
	var s Stats
	err := d.b.View(func(tx *bolt.Tx) error {
		sb := tx.Bucket([]byte(statsBucket))
		if sb == nil {
			return nil
		}
		raw := sb.Get([]byte(ns))
		if raw == nil {
			return nil
		}
		return json.Unmarshal(raw, &s)
	})
	return s, err
}

// Namespaces lists the namespaces present in the index.
func (d *DB) Namespaces() ([]string, error) {
	var out []string
	err := d.b.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, _ *bolt.Bucket) error {
			if ns, ok := strings.CutPrefix(string(name), objPrefix); ok {
				out = append(out, ns)
			}
			return nil
		})
	})
	return out, err
}

// DropNamespace removes a namespace and its stats. Used when a site is
// removed from the config, so the index does not grow forever.
func (d *DB) DropNamespace(ns string) error {
	d.mu.Lock()
	delete(d.pending, ns)
	d.mu.Unlock()

	return d.b.Update(func(tx *bolt.Tx) error {
		if tx.Bucket(objKey(ns)) != nil {
			if err := tx.DeleteBucket(objKey(ns)); err != nil {
				return err
			}
		}
		if sb := tx.Bucket([]byte(statsBucket)); sb != nil {
			return sb.Delete([]byte(ns))
		}
		return nil
	})
}

// Path returns the index file path.
func (d *DB) Path() string { return d.b.Path() }

// atimeLoop persists accumulated access times until Close.
func (d *DB) atimeLoop() {
	t := time.NewTicker(atimeFlushInterval)
	defer t.Stop()
	for {
		select {
		case <-d.stop:
			return
		case <-t.C:
			d.flushATimes()
		}
	}
}

// flushATimes writes every pending access time in one transaction.
// Entries deleted meanwhile are silently dropped.
func (d *DB) flushATimes() {
	d.mu.Lock()
	pending := d.pending
	d.pending = make(map[string]map[string]time.Time)
	d.mu.Unlock()

	if len(pending) == 0 {
		return
	}
	// A failure here is not worth propagating: the only casualty is
	// eviction ordering, and the next read re-records the access.
	_ = d.b.Update(func(tx *bolt.Tx) error {
		for ns, keys := range pending {
			ob := tx.Bucket(objKey(ns))
			if ob == nil {
				continue
			}
			for key, at := range keys {
				raw := ob.Get([]byte(key))
				if raw == nil {
					continue
				}
				var e Entry
				if err := json.Unmarshal(raw, &e); err != nil {
					continue
				}
				if !at.After(e.ATime) {
					continue
				}
				e.ATime = at
				next, err := json.Marshal(e)
				if err != nil {
					continue
				}
				if err := ob.Put([]byte(key), next); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// applyStats folds the transition old -> new into the namespace
// counters. Either side may be nil (insert or delete). It runs inside
// the caller's write transaction, so counters and objects commit
// together or not at all.
func applyStats(tx *bolt.Tx, ns string, old, cur *Entry) error {
	sb, err := tx.CreateBucketIfNotExists([]byte(statsBucket))
	if err != nil {
		return err
	}
	var s Stats
	if raw := sb.Get([]byte(ns)); raw != nil {
		if err := json.Unmarshal(raw, &s); err != nil {
			return fmt.Errorf("index: corrupt stats for %s: %w", ns, err)
		}
	}
	if old != nil {
		s.add(old, -1)
	}
	if cur != nil {
		s.add(cur, +1)
	}
	raw, err := json.Marshal(s)
	if err != nil {
		return err
	}
	return sb.Put([]byte(ns), raw)
}

// add folds one entry into the counters with the given sign.
func (s *Stats) add(e *Entry, sign int64) {
	s.Objects += sign
	s.Bytes += sign * e.Size
	if e.State.Local() {
		s.LocalObjects += sign
		s.LocalBytes += sign * e.Size
	}
	if e.State == Dirty || e.State == Uploading {
		s.DirtyObjects += sign
		s.DirtyBytes += sign * e.Size
	}
}

// objKey is the bbolt bucket name holding a namespace's objects.
func objKey(ns string) []byte { return []byte(objPrefix + ns) }

// Namespace scopes an index namespace by the subsystem that owns it.
//
// One machine can run both roles — a single box for a small
// deployment, or for a first test — and then the store and the agent
// share one index file. Without a scope they would share more than
// that: a site named like a bucket would land on the same entries, and
// the store's rebuild would wipe the agent's namespace on its way
// past. The scope is what keeps two subsystems in one file from being
// two subsystems in one namespace.
func Namespace(scope, name string) string { return scope + ":" + name }

// Scopes. Kept short because they prefix every key in the file.
const (
	ScopeStore = "store"
	ScopeAgent = "agent"
)

// InScope reports whether a namespace belongs to a scope.
func InScope(ns, scope string) bool { return strings.HasPrefix(ns, scope+":") }

// NameOf strips the scope from a namespace, for display.
func NameOf(ns string) string {
	if _, name, ok := strings.Cut(ns, ":"); ok {
		return name
	}
	return ns
}
