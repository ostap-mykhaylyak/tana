package store

import (
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/ostap-mykhaylyak/tana/internal/index"
	"github.com/ostap-mykhaylyak/tana/internal/journal"
)

// Integrity checking.
//
// Two questions, deliberately separate because they cost differently.
//
// Fsck asks whether the index agrees with the journal and the blobs.
// It reads metadata only, so it runs in seconds on a store with
// millions of objects, and it can rebuild the index outright: the
// journal is the durable record, and the index is derived from it.
//
// Scrub asks whether the bytes are still the bytes. That means reading
// every blob and hashing it, so it is bounded by disk throughput.
// Content addressing makes the check itself trivial — a blob's name IS
// its checksum, so there is no manifest to keep, and no chance of the
// manifest rotting alongside the data it describes.

// FsckReport is what one metadata check found.
type FsckReport struct {
	JournalRecords int   `json:"journal_records"`
	Objects        int64 `json:"objects"`
	References     int64 `json:"references"`
	// MissingBlobs are objects whose content is not on disk. This is
	// the one finding that means data loss rather than bookkeeping.
	MissingBlobs []string `json:"missing_blobs,omitempty"`
	// UnreferencedBlobs are on disk with nothing pointing at them. The
	// collector removes them in the normal course of things; a large
	// number here means it has not run, not that anything is wrong.
	UnreferencedBlobs int64 `json:"unreferenced_blobs"`
	// StaleRefs are reference records for blobs that no longer exist.
	StaleRefs int64         `json:"stale_refs"`
	Rebuilt   bool          `json:"rebuilt"`
	Duration  time.Duration `json:"duration"`
}

// Healthy reports whether nothing was found that needs a human.
func (r FsckReport) Healthy() bool { return len(r.MissingBlobs) == 0 && r.StaleRefs == 0 }

// Fsck checks the index against the journal and the blob store.
//
// With rebuild set, the index is discarded and replayed from the
// journal first. That is the recovery path for a lost or corrupted
// index, and it is safe precisely because the index never held
// anything the journal does not: losing it costs time, not data.
func (s *Store) Fsck(rebuild bool) (FsckReport, error) {
	start := time.Now()
	var rep FsckReport

	if rebuild {
		n, err := s.rebuildIndex()
		if err != nil {
			return rep, err
		}
		rep.JournalRecords = n
		rep.Rebuilt = true
	}

	// Every object must have its blob. Collected here rather than
	// reported one at a time so the caller sees the shape of the
	// problem instead of the first instance of it.
	namespaces, err := s.idx.Namespaces()
	if err != nil {
		return rep, err
	}
	referenced := map[string]bool{}
	for _, ns := range namespaces {
		if err := s.idx.Walk(ns, func(e index.Entry) error {
			rep.Objects++
			if e.Hash == "" {
				return nil
			}
			referenced[e.Hash] = true
			if _, ok, err := s.blobs.Stat(e.Hash); err != nil {
				return err
			} else if !ok {
				rep.MissingBlobs = append(rep.MissingBlobs, ns+"/"+e.Key)
			}
			return nil
		}); err != nil {
			return rep, err
		}
	}
	sort.Strings(rep.MissingBlobs)

	// Reference records pointing at blobs that are gone.
	if err := s.idx.WalkRefs(func(hash string, r index.Ref) error {
		rep.References++
		if r.Count <= 0 {
			return nil // already unreferenced; the collector's business
		}
		if _, ok, err := s.blobs.Stat(hash); err != nil {
			return err
		} else if !ok {
			rep.StaleRefs++
		}
		return nil
	}); err != nil {
		return rep, err
	}

	// Blobs nothing points at.
	if err := s.blobs.Walk(func(hash string, _ int64, _ time.Time, bad bool) error {
		if bad || referenced[hash] {
			return nil
		}
		rep.UnreferencedBlobs++
		return nil
	}); err != nil {
		return rep, err
	}

	rep.Duration = time.Since(start)
	return rep, nil
}

// rebuildIndex discards the derived index and replays the journal.
func (s *Store) rebuildIndex() (int, error) {
	namespaces, err := s.idx.Namespaces()
	if err != nil {
		return 0, err
	}
	for _, ns := range namespaces {
		if err := s.idx.DropNamespace(ns); err != nil {
			return 0, err
		}
	}
	var refs []string
	if err := s.idx.WalkRefs(func(hash string, _ index.Ref) error {
		refs = append(refs, hash)
		return nil
	}); err != nil {
		return 0, err
	}
	if err := s.idx.Update(func(tx *index.Tx) error {
		for _, h := range refs {
			if err := tx.DropRef(h); err != nil {
				return err
			}
		}
		return tx.SetMeta(appliedSeqKey, []byte("0"))
	}); err != nil {
		return 0, err
	}

	count := 0
	if err := s.idx.Update(func(tx *index.Tx) error {
		return s.jrnl.Replay(1, func(r journal.Record) error {
			if err := s.apply(tx, r); err != nil {
				return err
			}
			count++
			return nil
		})
	}); err != nil {
		return count, fmt.Errorf("store: rebuild: %w", err)
	}
	s.svcLog.Info("index rebuilt from the journal", "records", count)
	return count, nil
}

// ScrubReport is what one content check found.
type ScrubReport struct {
	Blobs int64 `json:"blobs"`
	Bytes int64 `json:"bytes"`
	// Corrupt are blobs whose content no longer hashes to their name.
	// On a store with redundancy underneath, this is what tells you the
	// redundancy did not do its job.
	Corrupt []string `json:"corrupt,omitempty"`
	// Foreign are files under blobs/ that tana did not write. They are
	// counted and left alone.
	Foreign  int64         `json:"foreign"`
	Duration time.Duration `json:"duration"`
}

// Healthy reports whether every blob still matches its name.
func (r ScrubReport) Healthy() bool { return len(r.Corrupt) == 0 }

// Scrub verifies every blob's content against its name.
//
// progress, when non-nil, is called every so often with the running
// count, because on a large store this takes as long as reading the
// whole store takes and silence is indistinguishable from a hang.
func (s *Store) Scrub(progress func(blobs, bytes int64)) (ScrubReport, error) {
	start := time.Now()
	var rep ScrubReport

	var hashes []string
	var sizes []int64
	if err := s.blobs.Walk(func(hash string, size int64, _ time.Time, bad bool) error {
		if bad {
			rep.Foreign++
			return nil
		}
		hashes = append(hashes, hash)
		sizes = append(sizes, size)
		return nil
	}); err != nil {
		return rep, err
	}

	next := time.Now().Add(5 * time.Second)
	for i, hash := range hashes {
		ok, err := s.blobs.Verify(hash)
		if err != nil {
			return rep, err
		}
		rep.Blobs++
		rep.Bytes += sizes[i]
		if !ok {
			rep.Corrupt = append(rep.Corrupt, hash)
			s.svcLog.Error("blob content does not match its hash", "hash", hash)
		}
		if progress != nil && time.Now().After(next) {
			progress(rep.Blobs, rep.Bytes)
			next = time.Now().Add(5 * time.Second)
		}
	}
	sort.Strings(rep.Corrupt)
	rep.Duration = time.Since(start)
	return rep, nil
}

// AppliedSeqString renders the applied journal position, for reports.
func (s *Store) AppliedSeqString() string {
	n, err := s.appliedSeq()
	if err != nil {
		return "unknown"
	}
	return strconv.FormatUint(n, 10)
}
