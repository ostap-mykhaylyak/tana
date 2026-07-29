// Package store is the S3 service side: the half of tana that owns
// the bytes.
//
// It composes three pieces that each do one thing. internal/blob holds
// immutable content-addressed files. internal/journal records every
// mutation before it is acknowledged. internal/index maps keys to
// content hashes and counts how many keys point at each one.
//
// The ordering between them is the whole design:
//
//	blob durable  ->  journal durable  ->  index committed  ->  ack
//
// Each step is recoverable from the one before it. A crash after the
// blob lands leaves an unreferenced file, which the collector removes.
// A crash after the journal entry lands leaves an index that is behind,
// which Recover replays forward. There is no ordering in which a client
// is told a write succeeded and the bytes are not there — which is the
// only promise a storage system really makes.
package store

import (
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/ostap-mykhaylyak/tana/internal/blob"
	"github.com/ostap-mykhaylyak/tana/internal/config"
	"github.com/ostap-mykhaylyak/tana/internal/index"
	"github.com/ostap-mykhaylyak/tana/internal/journal"
)

// appliedSeqKey names the index metadata holding how far the journal
// has been applied. It is the join between the two durable artifacts.
const appliedSeqKey = "store.applied_seq"

// journalDir is the journal's subdirectory under store.data.
const journalDir = "journal"

// ErrNoSuchBucket is returned for a bucket that is not configured.
type ErrNoSuchBucket struct{ Name string }

func (e ErrNoSuchBucket) Error() string { return "no such bucket: " + e.Name }

// ErrNoSuchKey is returned for a key that does not exist.
type ErrNoSuchKey struct{ Bucket, Key string }

func (e ErrNoSuchKey) Error() string { return "no such key: " + e.Bucket + "/" + e.Key }

// Store is the object store.
type Store struct {
	blobs *blob.Store
	jrnl  *journal.Journal
	idx   *index.DB

	svcLog  *slog.Logger
	xferLog *slog.Logger

	mu      sync.RWMutex
	buckets map[string]config.Bucket
	gc      config.GC
}

// New opens the blob store and journal under cfg.Data and returns a
// Store ready to Recover.
func New(cfg config.Store, idx *index.DB, svcLog, xferLog *slog.Logger) (*Store, error) {
	blobs, err := blob.Open(cfg.Data)
	if err != nil {
		return nil, err
	}
	jrnl, err := journal.Open(filepath.Join(cfg.Data, journalDir))
	if err != nil {
		return nil, err
	}
	s := &Store{
		blobs:   blobs,
		jrnl:    jrnl,
		idx:     idx,
		svcLog:  svcLog,
		xferLog: xferLog,
	}
	s.Configure(cfg)
	return s, nil
}

// Configure applies a (possibly reloaded) configuration. Only the
// tenant table and the collection schedule are hot-swappable; the data
// root and the journal are bound at startup.
func (s *Store) Configure(cfg config.Store) {
	buckets := make(map[string]config.Bucket, len(cfg.Buckets))
	for _, b := range cfg.Buckets {
		buckets[b.Name] = b
	}
	s.mu.Lock()
	s.buckets, s.gc = buckets, cfg.GC
	s.mu.Unlock()
}

// Bucket returns a configured bucket by name.
func (s *Store) Bucket(name string) (config.Bucket, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.buckets[name]
	return b, ok
}

// BucketByAccessKey resolves credentials to the single bucket they may
// touch.
//
// One key pair per bucket is the whole authorization model, and it is
// enough: a site's agent has no business reading another site's media,
// and a model with exactly one rule is a model nobody misconfigures.
func (s *Store) BucketByAccessKey(accessKey string) (config.Bucket, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, b := range s.buckets {
		if b.AccessKey == accessKey {
			return b, true
		}
	}
	return config.Bucket{}, false
}

// Buckets lists the configured bucket names.
func (s *Store) Buckets() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.buckets))
	for name := range s.buckets {
		out = append(out, name)
	}
	return out
}

// Blobs exposes the blob store, for the scrub and for --status.
func (s *Store) Blobs() *blob.Store { return s.blobs }

// Journal exposes the journal, for replication (M6).
func (s *Store) Journal() *journal.Journal { return s.jrnl }

// Recover replays journal records the index has not applied yet and
// returns how many it applied.
//
// This runs at every startup, not only after a crash: it is cheap when
// there is nothing to do, and a recovery path that only executes after
// a disaster is a recovery path nobody has tested.
func (s *Store) Recover() (int, error) {
	applied, err := s.appliedSeq()
	if err != nil {
		return 0, err
	}
	last := s.jrnl.LastSeq()
	if applied >= last {
		return 0, nil
	}

	count := 0
	err = s.idx.Update(func(tx *index.Tx) error {
		return s.jrnl.Replay(applied+1, func(r journal.Record) error {
			if err := s.apply(tx, r); err != nil {
				return err
			}
			count++
			return nil
		})
	})
	if err != nil {
		return 0, fmt.Errorf("store: recover: %w", err)
	}
	s.svcLog.Info("journal replayed", "records", count, "from", applied+1, "to", last)
	return count, nil
}

// apply folds one journal record into the index. It is idempotent:
// records carry everything needed to reach the target state without
// consulting what is already there, so replaying an overlapping range
// is harmless.
func (s *Store) apply(tx *index.Tx, r journal.Record) error {
	switch r.Op {
	case journal.OpPut:
		if err := s.bind(tx, r.Bucket, r.Key, r.Hash, r.ETag, r.Size, r.MTime, r.Time); err != nil {
			return err
		}
	case journal.OpDelete:
		if err := s.unbind(tx, r.Bucket, r.Key, r.Time); err != nil {
			return err
		}
	case journal.OpGC:
		if err := tx.DropRef(r.Hash); err != nil {
			return err
		}
	default:
		return fmt.Errorf("store: unknown journal op %q at seq %d", r.Op, r.Seq)
	}
	return tx.SetMeta(appliedSeqKey, []byte(strconv.FormatUint(r.Seq, 10)))
}

// bind points a key at a hash, moving the reference counts to match.
func (s *Store) bind(tx *index.Tx, bucket, key, hash, etag string, size int64, mtime, now time.Time) error {
	old, existed, err := tx.Get(bucket, key)
	if err != nil {
		return err
	}
	// Rebinding a key to content it already has must not inflate the
	// reference count, or the blob becomes uncollectable forever.
	if existed && old.Hash == hash {
		return tx.Put(bucket, index.Entry{
			Key: key, Size: size, ModTime: mtime, Mode: old.Mode,
			Hash: hash, ETag: etag, State: index.Synced, ATime: now, Pinned: old.Pinned,
		})
	}
	if existed && old.Hash != "" {
		if _, err := tx.Ref(old.Hash, -1, now); err != nil {
			return err
		}
	}
	if _, err := tx.Ref(hash, +1, now); err != nil {
		return err
	}
	return tx.Put(bucket, index.Entry{
		Key: key, Size: size, ModTime: mtime, Mode: 0o644,
		Hash: hash, ETag: etag, State: index.Synced, ATime: now,
	})
}

// unbind removes a key and releases its reference.
func (s *Store) unbind(tx *index.Tx, bucket, key string, now time.Time) error {
	old, existed, err := tx.Delete(bucket, key)
	if err != nil || !existed {
		return err
	}
	if old.Hash == "" {
		return nil
	}
	_, err = tx.Ref(old.Hash, -1, now)
	return err
}

// PutOptions tunes a write.
type PutOptions struct {
	// MTime is the object's modification time. Zero means now.
	MTime time.Time
	// ETag overrides the entity tag handed back to the client. Empty
	// means the content's md5, which is what S3 returns for a
	// whole-object upload. A multipart completion sets it, because its
	// tag depends on how the upload was split and cannot be recomputed
	// from the assembled bytes.
	ETag string
	// ExpectHash, when set, is compared against the content's sha256
	// after the blob lands. It is how a client's declared
	// x-amz-content-sha256 is enforced without buffering the body:
	// there is no cheaper check, because the store hashed the content
	// on the way in anyway.
	ExpectHash string
}

// Put stores an object and returns its index entry.
func (s *Store) Put(bucket, key string, r io.Reader, mtime time.Time) (index.Entry, error) {
	return s.PutWith(bucket, key, r, PutOptions{MTime: mtime})
}

// ErrDigestMismatch reports that stored content did not match what the
// client said it was sending.
type ErrDigestMismatch struct{ Want, Got string }

func (e ErrDigestMismatch) Error() string {
	return "content hash mismatch: client declared " + e.Want + ", stored " + e.Got
}

// PutWith stores an object with options.
func (s *Store) PutWith(bucket, key string, r io.Reader, opt PutOptions) (index.Entry, error) {
	if _, ok := s.Bucket(bucket); !ok {
		return index.Entry{}, ErrNoSuchBucket{Name: bucket}
	}
	if key == "" {
		return index.Entry{}, fmt.Errorf("store: empty key")
	}
	mtime := opt.MTime
	if mtime.IsZero() {
		mtime = time.Now().UTC()
	}

	info, err := s.blobs.Put(r)
	if err != nil {
		return index.Entry{}, err
	}
	if opt.ExpectHash != "" && opt.ExpectHash != info.Hash {
		// The blob stays: it is unreferenced, so the collector takes it
		// after the grace period, and removing it here could delete
		// content a concurrent, honest Put just stored.
		return index.Entry{}, ErrDigestMismatch{Want: opt.ExpectHash, Got: info.Hash}
	}

	etag := opt.ETag
	if etag == "" {
		etag = info.MD5
	}

	seq, err := s.jrnl.Append(journal.Record{
		Op: journal.OpPut, Bucket: bucket, Key: key,
		Hash: info.Hash, ETag: etag, Size: info.Size, MTime: mtime,
	})
	if err != nil {
		// The blob is durable but unreferenced. Leaving it is correct:
		// the collector will remove it once it is older than the grace
		// period, and deleting it here would race a concurrent Put of
		// the same content that DID get journalled.
		return index.Entry{}, err
	}

	now := time.Now().UTC()
	if err := s.idx.Update(func(tx *index.Tx) error {
		if err := s.bind(tx, bucket, key, info.Hash, etag, info.Size, mtime, now); err != nil {
			return err
		}
		return tx.SetMeta(appliedSeqKey, []byte(strconv.FormatUint(seq, 10)))
	}); err != nil {
		return index.Entry{}, fmt.Errorf("store: index put: %w", err)
	}

	s.xferLog.Info("put", "bucket", bucket, "key", key,
		"size", info.Size, "hash", info.Hash, "deduped", info.Deduped, "seq", seq)
	return index.Entry{
		Key: key, Size: info.Size, ModTime: mtime, Mode: 0o644,
		Hash: info.Hash, ETag: etag, State: index.Synced, ATime: now,
	}, nil
}

// Head returns an object's metadata without touching its bytes. This
// is the call the whole design optimizes for.
func (s *Store) Head(bucket, key string) (index.Entry, error) {
	if _, ok := s.Bucket(bucket); !ok {
		return index.Entry{}, ErrNoSuchBucket{Name: bucket}
	}
	e, ok, err := s.idx.Get(bucket, key)
	if err != nil {
		return index.Entry{}, err
	}
	if !ok {
		return index.Entry{}, ErrNoSuchKey{Bucket: bucket, Key: key}
	}
	return e, nil
}

// Get returns an object's metadata and a reader for its content. The
// caller closes the reader.
func (s *Store) Get(bucket, key string) (index.Entry, io.ReadCloser, error) {
	e, err := s.Head(bucket, key)
	if err != nil {
		return index.Entry{}, nil, err
	}
	f, err := s.blobs.Open(e.Hash)
	if err != nil {
		// The index says it is there and the disk says otherwise. That
		// is the one inconsistency worth shouting about, because it
		// means either a scrub finding or someone deleting by hand.
		s.svcLog.Error("object references a missing blob",
			"bucket", bucket, "key", key, "hash", e.Hash)
		return index.Entry{}, nil, err
	}
	s.idx.Touch(bucket, key, time.Now())
	return e, f, nil
}

// Delete removes a key. Deleting what is not there succeeds, as S3
// requires.
func (s *Store) Delete(bucket, key string) error {
	if _, ok := s.Bucket(bucket); !ok {
		return ErrNoSuchBucket{Name: bucket}
	}
	if _, ok, err := s.idx.Get(bucket, key); err != nil {
		return err
	} else if !ok {
		return nil
	}

	seq, err := s.jrnl.Append(journal.Record{Op: journal.OpDelete, Bucket: bucket, Key: key})
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	if err := s.idx.Update(func(tx *index.Tx) error {
		if err := s.unbind(tx, bucket, key, now); err != nil {
			return err
		}
		return tx.SetMeta(appliedSeqKey, []byte(strconv.FormatUint(seq, 10)))
	}); err != nil {
		return fmt.Errorf("store: index delete: %w", err)
	}
	s.xferLog.Info("delete", "bucket", bucket, "key", key, "seq", seq)
	return nil
}

// List calls fn for every object in a bucket whose key starts with
// prefix, in key order. It reads only the index, never the disk.
func (s *Store) List(bucket, prefix string, fn func(index.Entry) error) error {
	if _, ok := s.Bucket(bucket); !ok {
		return ErrNoSuchBucket{Name: bucket}
	}
	if prefix == "" {
		return s.idx.Walk(bucket, fn)
	}
	return s.idx.WalkPrefix(bucket, prefix, fn)
}

// Stats returns a bucket's index counters.
func (s *Store) Stats(bucket string) (index.Stats, error) { return s.idx.Stats(bucket) }

// AppliedSeq reports how far the journal has been folded into the
// index. Together with the journal's last sequence it answers "is the
// index caught up", which is the first question worth asking after a
// crash.
func (s *Store) AppliedSeq() (uint64, error) { return s.appliedSeq() }

// LastSeq reports the highest sequence written to the journal.
func (s *Store) LastSeq() uint64 { return s.jrnl.LastSeq() }

// appliedSeq reads how far the journal has been applied to the index.
func (s *Store) appliedSeq() (uint64, error) {
	raw, err := s.idx.Meta(appliedSeqKey)
	if err != nil || len(raw) == 0 {
		return 0, err
	}
	v, err := strconv.ParseUint(string(raw), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("store: unreadable %s %q: %w", appliedSeqKey, raw, err)
	}
	return v, nil
}

// Close releases the journal. The index is owned by the caller.
func (s *Store) Close() error { return s.jrnl.Close() }
