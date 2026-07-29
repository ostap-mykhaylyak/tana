package index

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"
)

// refsBucket holds one reference count per blob hash.
const refsBucket = "refs"

// Ref is how many keys currently point at a blob, and when the count
// last reached zero.
//
// A blob is not deleted the moment it loses its last reference. The
// grace period between UnrefAt and collection is the difference
// between an accidental mass delete you can undo and one you cannot,
// and it costs only disk that was already allocated.
type Ref struct {
	Count   int64     `json:"count"`
	UnrefAt time.Time `json:"unref_at,omitempty"`
}

// Collectable reports whether the blob has been unreferenced for
// longer than grace and can be removed.
func (r Ref) Collectable(now time.Time, grace time.Duration) bool {
	return r.Count <= 0 && !r.UnrefAt.IsZero() && now.Sub(r.UnrefAt) >= grace
}

// Tx is a write transaction over the index. It exists so that the
// things which must be true together — an object exists, its blob is
// referenced, the journal position that produced both — commit
// together or not at all. Composing them from separate calls would
// leave a window where a crash produces a blob nobody references, or
// worse, a key pointing at a blob already collected.
type Tx struct{ tx *bolt.Tx }

// Update runs fn in a write transaction, committing on success.
func (d *DB) Update(fn func(*Tx) error) error {
	return d.b.Update(func(tx *bolt.Tx) error { return fn(&Tx{tx: tx}) })
}

// View runs fn in a read transaction.
func (d *DB) View(fn func(*Tx) error) error {
	return d.b.View(func(tx *bolt.Tx) error { return fn(&Tx{tx: tx}) })
}

// Put inserts or replaces an entry, keeping the namespace counters in
// step within the same transaction.
func (t *Tx) Put(ns string, e Entry) error {
	if e.Key == "" {
		return fmt.Errorf("index: empty key")
	}
	if e.ATime.IsZero() {
		e.ATime = time.Now()
	}
	ob, err := t.tx.CreateBucketIfNotExists(objKey(ns))
	if err != nil {
		return err
	}
	old, err := decodeAt(ob, ns, e.Key)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(e)
	if err != nil {
		return err
	}
	if err := ob.Put([]byte(e.Key), raw); err != nil {
		return err
	}
	return applyStats(t.tx, ns, old, &e)
}

// Get reads an entry. Pending access times are NOT merged here: inside
// a transaction the caller wants what is committed.
func (t *Tx) Get(ns, key string) (Entry, bool, error) {
	ob := t.tx.Bucket(objKey(ns))
	if ob == nil {
		return Entry{}, false, nil
	}
	e, err := decodeAt(ob, ns, key)
	if err != nil || e == nil {
		return Entry{}, false, err
	}
	return *e, true, nil
}

// Delete removes an entry, returning what was there. A missing key is
// not an error: callers reconciling against a filesystem should not
// have to check first.
func (t *Tx) Delete(ns, key string) (Entry, bool, error) {
	ob := t.tx.Bucket(objKey(ns))
	if ob == nil {
		return Entry{}, false, nil
	}
	old, err := decodeAt(ob, ns, key)
	if err != nil || old == nil {
		return Entry{}, false, err
	}
	if err := ob.Delete([]byte(key)); err != nil {
		return Entry{}, false, err
	}
	return *old, true, applyStats(t.tx, ns, old, nil)
}

// SetState transitions an object without rewriting its metadata.
func (t *Tx) SetState(ns, key string, s State) (bool, error) {
	ob := t.tx.Bucket(objKey(ns))
	if ob == nil {
		return false, nil
	}
	old, err := decodeAt(ob, ns, key)
	if err != nil || old == nil {
		return false, err
	}
	next := *old
	next.State = s
	raw, err := json.Marshal(next)
	if err != nil {
		return false, err
	}
	if err := ob.Put([]byte(key), raw); err != nil {
		return false, err
	}
	return true, applyStats(t.tx, ns, old, &next)
}

// Ref adjusts a blob's reference count by delta and returns the new
// value. Reaching zero stamps UnrefAt, which starts the grace period;
// coming back above zero clears it, so a blob re-referenced during the
// grace window is never collected.
func (t *Tx) Ref(hash string, delta int64, now time.Time) (int64, error) {
	if hash == "" {
		return 0, fmt.Errorf("index: empty hash")
	}
	rb, err := t.tx.CreateBucketIfNotExists([]byte(refsBucket))
	if err != nil {
		return 0, err
	}
	var r Ref
	if raw := rb.Get([]byte(hash)); raw != nil {
		if err := json.Unmarshal(raw, &r); err != nil {
			return 0, fmt.Errorf("index: corrupt ref %s: %w", hash, err)
		}
	}
	r.Count += delta
	switch {
	case r.Count <= 0 && r.UnrefAt.IsZero():
		r.UnrefAt = now
	case r.Count > 0:
		r.UnrefAt = time.Time{}
	}
	raw, err := json.Marshal(r)
	if err != nil {
		return 0, err
	}
	return r.Count, rb.Put([]byte(hash), raw)
}

// RefOf reads a blob's reference record.
func (t *Tx) RefOf(hash string) (Ref, bool, error) {
	rb := t.tx.Bucket([]byte(refsBucket))
	if rb == nil {
		return Ref{}, false, nil
	}
	raw := rb.Get([]byte(hash))
	if raw == nil {
		return Ref{}, false, nil
	}
	var r Ref
	if err := json.Unmarshal(raw, &r); err != nil {
		return Ref{}, false, fmt.Errorf("index: corrupt ref %s: %w", hash, err)
	}
	return r, true, nil
}

// DropRef removes a reference record entirely, after its blob is gone.
func (t *Tx) DropRef(hash string) error {
	rb := t.tx.Bucket([]byte(refsBucket))
	if rb == nil {
		return nil
	}
	return rb.Delete([]byte(hash))
}

// SetMeta stores a small daemon-level value, such as the journal
// position the index has caught up to.
func (t *Tx) SetMeta(key string, value []byte) error {
	mb, err := t.tx.CreateBucketIfNotExists([]byte(metaBucket))
	if err != nil {
		return err
	}
	return mb.Put([]byte(key), value)
}

// DelMeta removes a value written by SetMeta.
func (t *Tx) DelMeta(key string) error {
	mb := t.tx.Bucket([]byte(metaBucket))
	if mb == nil {
		return nil
	}
	return mb.Delete([]byte(key))
}

// WalkMeta calls fn for every metadata key with the given prefix.
func (t *Tx) WalkMeta(prefix string, fn func(key string, value []byte) error) error {
	mb := t.tx.Bucket([]byte(metaBucket))
	if mb == nil {
		return nil
	}
	c := mb.Cursor()
	p := []byte(prefix)
	for k, v := c.Seek(p); k != nil && strings.HasPrefix(string(k), prefix); k, v = c.Next() {
		if err := fn(string(k), append([]byte(nil), v...)); err != nil {
			return err
		}
	}
	return nil
}

// Meta reads a value written by SetMeta.
func (t *Tx) Meta(key string) []byte {
	mb := t.tx.Bucket([]byte(metaBucket))
	if mb == nil {
		return nil
	}
	v := mb.Get([]byte(key))
	if v == nil {
		return nil
	}
	// bbolt values are only valid for the life of the transaction.
	return append([]byte(nil), v...)
}

// Meta reads a daemon-level value outside a transaction.
func (d *DB) Meta(key string) ([]byte, error) {
	var out []byte
	err := d.View(func(t *Tx) error {
		out = t.Meta(key)
		return nil
	})
	return out, err
}

// SetMeta writes a daemon-level value outside a transaction.
func (d *DB) SetMeta(key string, value []byte) error {
	return d.Update(func(t *Tx) error { return t.SetMeta(key, value) })
}

// WalkRefs calls fn for every reference record. fn must not call back
// into the DB: it runs inside a read transaction.
func (d *DB) WalkRefs(fn func(hash string, r Ref) error) error {
	return d.b.View(func(tx *bolt.Tx) error {
		rb := tx.Bucket([]byte(refsBucket))
		if rb == nil {
			return nil
		}
		return rb.ForEach(func(k, raw []byte) error {
			var r Ref
			if err := json.Unmarshal(raw, &r); err != nil {
				return fmt.Errorf("index: corrupt ref %s: %w", k, err)
			}
			return fn(string(k), r)
		})
	})
}

// decodeAt reads and decodes one entry, returning nil when absent.
func decodeAt(ob *bolt.Bucket, ns, key string) (*Entry, error) {
	raw := ob.Get([]byte(key))
	if raw == nil {
		return nil, nil
	}
	var e Entry
	if err := json.Unmarshal(raw, &e); err != nil {
		return nil, fmt.Errorf("index: corrupt entry %s/%s: %w", ns, key, err)
	}
	return &e, nil
}
