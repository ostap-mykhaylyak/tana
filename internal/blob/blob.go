// Package blob is the content-addressed store: the only place in tana
// that owns bytes.
//
// A blob's name is the hex sha256 of its content, which buys three
// things at once. Identical uploads across sites — and WordPress
// installs share a great many identical files — occupy the disk once.
// Blobs are immutable, so nothing ever has to be locked or versioned.
// And integrity checking is intrinsic: the filename IS the checksum,
// so a scrub needs no separate manifest to compare against.
//
// The write path is the part worth reading. A PUT is not acknowledged
// until the bytes are recoverable after a power cut, which takes more
// than writing and closing a file: the data must reach the platter,
// and so must the directory entry that names it. Skipping either turns
// durability into a claim rather than a property.
package blob

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Directory names under the store root.
const (
	blobsDir = "blobs"
	tmpDir   = "tmp"

	// fanout splits the hash into nested directories so no single
	// directory holds millions of entries, which several filesystems
	// handle badly and every operator handles badly.
	fanout = 2
	// fanoutLen is how many hex characters each fanout level consumes.
	fanoutLen = 2

	dirPerm  = 0o750
	filePerm = 0o640
)

// ErrNotFound is returned when a hash is not in the store.
var ErrNotFound = errors.New("blob not found")

// Info describes a stored blob.
type Info struct {
	Hash string
	Size int64
	// Deduped reports that the content was already present, so the
	// upload cost a hash and nothing else.
	Deduped bool
}

// Store is a content-addressed blob store rooted at a directory.
type Store struct {
	root string
}

// Open prepares the store rooted at root, creating its directories.
func Open(root string) (*Store, error) {
	s := &Store{root: root}
	for _, d := range []string{s.blobs(), s.tmp()} {
		if err := os.MkdirAll(d, dirPerm); err != nil {
			return nil, fmt.Errorf("blob store: create %s: %w", d, err)
		}
	}
	// A crash leaves partial files in tmp/. They are unreferenced by
	// construction — nothing learns their name until the rename — so
	// clearing them at startup is always safe.
	if err := s.clearTmp(); err != nil {
		return nil, err
	}
	return s, nil
}

// Root returns the store root.
func (s *Store) Root() string { return s.root }

func (s *Store) blobs() string { return filepath.Join(s.root, blobsDir) }
func (s *Store) tmp() string   { return filepath.Join(s.root, tmpDir) }

// Path returns where a hash lives, without checking that it does.
// Returns "" for a malformed hash rather than slicing it: this is the
// only path-building function in the package, so it must not be
// possible to reach the filesystem through it with arbitrary input.
func (s *Store) Path(hash string) string {
	if ValidHash(hash) != nil {
		return ""
	}
	parts := []string{s.blobs()}
	for i := 0; i < fanout; i++ {
		parts = append(parts, hash[i*fanoutLen:(i+1)*fanoutLen])
	}
	return filepath.Join(append(parts, hash)...)
}

// Put streams r into the store and returns its content hash.
//
// The sequence is deliberate and each step is load-bearing:
//
//	write to tmp/  the name is unknown to everyone until it is final
//	fsync(file)    the data itself reaches stable storage
//	rename()       atomic on the same filesystem: a blob is never
//	               half-visible, it either exists or does not
//	fsync(dir)     the directory entry reaches stable storage too;
//	               without this the file survives a power cut but its
//	               name does not, which is the same as losing it
func (s *Store) Put(r io.Reader) (Info, error) {
	tmp, err := s.newTemp()
	if err != nil {
		return Info{}, err
	}
	tmpName := tmp.Name()
	// Any early return must not leave the temp file behind.
	defer func() {
		tmp.Close()
		os.Remove(tmpName)
	}()

	h := sha256.New()
	size, err := io.Copy(io.MultiWriter(tmp, h), r)
	if err != nil {
		return Info{}, fmt.Errorf("blob store: write: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return Info{}, fmt.Errorf("blob store: sync: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return Info{}, fmt.Errorf("blob store: close: %w", err)
	}

	hash := hex.EncodeToString(h.Sum(nil))
	dst := s.Path(hash)

	// Dedup: identical content is already durable under this exact
	// name, so the cheapest correct thing is to drop what we just
	// wrote. Not overwriting also means a concurrent reader of the
	// existing blob is never disturbed.
	if _, err := os.Stat(dst); err == nil {
		return Info{Hash: hash, Size: size, Deduped: true}, nil
	}

	dir := filepath.Dir(dst)
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return Info{}, fmt.Errorf("blob store: create %s: %w", dir, err)
	}
	if err := os.Rename(tmpName, dst); err != nil {
		// Losing the race against a concurrent Put of the same content
		// is a success: the blob is there, and it is byte-identical.
		if _, statErr := os.Stat(dst); statErr == nil {
			return Info{Hash: hash, Size: size, Deduped: true}, nil
		}
		return Info{}, fmt.Errorf("blob store: rename: %w", err)
	}
	if err := syncDir(dir); err != nil {
		return Info{}, fmt.Errorf("blob store: sync dir: %w", err)
	}
	return Info{Hash: hash, Size: size}, nil
}

// Open returns a reader for a blob.
func (s *Store) Open(hash string) (*os.File, error) {
	if err := ValidHash(hash); err != nil {
		return nil, err
	}
	f, err := os.Open(s.Path(hash))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, hash)
	}
	return f, err
}

// Stat reports a blob's size and whether it exists.
func (s *Store) Stat(hash string) (int64, bool, error) {
	if err := ValidHash(hash); err != nil {
		return 0, false, err
	}
	fi, err := os.Stat(s.Path(hash))
	if errors.Is(err, fs.ErrNotExist) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return fi.Size(), true, nil
}

// Remove deletes a blob. Removing one that is not there is not an
// error: the garbage collector must be safe to run twice.
func (s *Store) Remove(hash string) error {
	if err := ValidHash(hash); err != nil {
		return err
	}
	err := os.Remove(s.Path(hash))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

// Walk calls fn for every blob in the store, in no particular order.
//
// The modification time is passed through because the collector needs
// it: a blob written moments ago may not be referenced yet, and age is
// what tells "not referenced" apart from "not referenced YET".
//
// Files whose name is not a valid hash are reported with bad set
// rather than skipped silently: something put them there, and the
// operator should hear about it.
func (s *Store) Walk(fn func(hash string, size int64, mod time.Time, bad bool) error) error {
	return filepath.WalkDir(s.blobs(), func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		name := d.Name()
		info, err := d.Info()
		if err != nil {
			return err
		}
		return fn(name, info.Size(), info.ModTime(), ValidHash(name) != nil)
	})
}

// Verify recomputes a blob's hash and reports whether it still matches
// its name. This is the whole of the scrub: with content addressing
// there is nothing else to compare against.
func (s *Store) Verify(hash string) (bool, error) {
	f, err := s.Open(hash)
	if err != nil {
		return false, err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return false, err
	}
	return hex.EncodeToString(h.Sum(nil)) == hash, nil
}

// newTemp creates a uniquely named file under tmp/.
func (s *Store) newTemp() (*os.File, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return nil, fmt.Errorf("blob store: temp name: %w", err)
	}
	p := filepath.Join(s.tmp(), hex.EncodeToString(b[:]))
	f, err := os.OpenFile(p, os.O_CREATE|os.O_EXCL|os.O_WRONLY, filePerm)
	if err != nil {
		return nil, fmt.Errorf("blob store: create temp: %w", err)
	}
	return f, nil
}

// clearTmp removes leftovers from a previous run.
func (s *Store) clearTmp() error {
	entries, err := os.ReadDir(s.tmp())
	if err != nil {
		return fmt.Errorf("blob store: read %s: %w", s.tmp(), err)
	}
	for _, e := range entries {
		if err := os.Remove(filepath.Join(s.tmp(), e.Name())); err != nil {
			return fmt.Errorf("blob store: clear temp: %w", err)
		}
	}
	return nil
}

// ValidHash checks that a string is a lowercase hex sha256.
//
// Every path in this package is built by concatenating a hash, so this
// is also the boundary that stops a crafted S3 key from becoming a
// path: nothing containing a separator or a dot can survive it.
func ValidHash(hash string) error {
	if len(hash) != sha256.Size*2 {
		return fmt.Errorf("invalid blob hash %q: want %d hex characters", hash, sha256.Size*2)
	}
	for i := 0; i < len(hash); i++ {
		c := hash[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return fmt.Errorf("invalid blob hash %q: not lowercase hex", hash)
		}
	}
	return nil
}

// HashOf returns the hex sha256 of b, for callers that already hold
// the whole content in memory.
func HashOf(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// Usage reports the number of blobs and the bytes they occupy.
func (s *Store) Usage() (count int64, bytes int64, err error) {
	err = s.Walk(func(_ string, size int64, _ time.Time, bad bool) error {
		if bad {
			return nil
		}
		count++
		bytes += size
		return nil
	})
	return count, bytes, err
}

// String makes a Store printable in logs without exposing internals.
func (s *Store) String() string { return "blob store at " + strings.TrimSuffix(s.root, "/") }
