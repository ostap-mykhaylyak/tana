package store

import (
	"fmt"
	"time"

	"github.com/ostap-mykhaylyak/tana/internal/index"
	"github.com/ostap-mykhaylyak/tana/internal/journal"
)

// GCStats is what one collection pass did.
type GCStats struct {
	// RefsScanned is how many reference records were examined.
	RefsScanned int64 `json:"refs_scanned"`
	// Collected is blobs removed because their last reference expired.
	Collected int64 `json:"collected"`
	// Orphans is blobs removed because nothing ever referenced them —
	// the residue of a crash between writing a blob and recording it.
	Orphans int64 `json:"orphans"`
	// Foreign is files under blobs/ whose name is not a hash. They are
	// counted and left alone: tana did not put them there and will not
	// presume to remove them.
	Foreign int64 `json:"foreign"`
	// BytesReclaimed is the disk freed.
	BytesReclaimed int64 `json:"bytes_reclaimed"`
	// Duration is how long the pass took.
	Duration time.Duration `json:"duration"`
}

// GC runs one collection pass.
//
// Nothing is removed the moment it becomes unreferenced. Everything
// waits out the configured grace period, which buys back the two
// mistakes that actually happen: a bulk delete someone regrets, and a
// crash landing between a blob and the record that references it. Both
// look identical to a collector without a clock, and both are
// unrecoverable if it acts immediately.
func (s *Store) GC(now time.Time) (GCStats, error) {
	start := time.Now()
	s.mu.RLock()
	grace := s.gc.Grace.Std()
	s.mu.RUnlock()

	var st GCStats
	if err := s.collectExpired(now, grace, &st); err != nil {
		return st, err
	}
	if err := s.collectOrphans(now, grace, &st); err != nil {
		return st, err
	}
	st.Duration = time.Since(start)

	if st.Collected > 0 || st.Orphans > 0 || st.Foreign > 0 {
		s.xferLog.Info("gc pass",
			"collected", st.Collected, "orphans", st.Orphans, "foreign", st.Foreign,
			"bytes_reclaimed", st.BytesReclaimed, "refs_scanned", st.RefsScanned,
			"duration", st.Duration.String())
	}
	return st, nil
}

// collectExpired removes blobs whose reference count reached zero
// longer ago than the grace period.
func (s *Store) collectExpired(now time.Time, grace time.Duration, st *GCStats) error {
	// Candidates are gathered first and acted on afterwards: the walk
	// holds a read transaction, and removing them inside it would
	// deadlock against the write transaction each removal needs.
	var candidates []string
	if err := s.idx.WalkRefs(func(hash string, r index.Ref) error {
		st.RefsScanned++
		if r.Collectable(now, grace) {
			candidates = append(candidates, hash)
		}
		return nil
	}); err != nil {
		return err
	}

	for _, hash := range candidates {
		size, exists, err := s.blobs.Stat(hash)
		if err != nil {
			return err
		}

		// Re-check under a write transaction. Between the walk and here
		// a client may have PUT the same content again, which brings
		// the count back above zero and clears the timer.
		var drop bool
		if err := s.idx.Update(func(tx *index.Tx) error {
			r, ok, err := tx.RefOf(hash)
			if err != nil || !ok {
				return err
			}
			if !r.Collectable(now, grace) {
				return nil
			}
			drop = true
			return tx.DropRef(hash)
		}); err != nil {
			return fmt.Errorf("store: gc drop ref %s: %w", hash, err)
		}
		if !drop {
			continue
		}

		if err := s.blobs.Remove(hash); err != nil {
			return fmt.Errorf("store: gc remove %s: %w", hash, err)
		}
		if _, err := s.jrnl.Append(journal.Record{Op: journal.OpGC, Hash: hash}); err != nil {
			return err
		}
		st.Collected++
		if exists {
			st.BytesReclaimed += size
		}
	}
	return nil
}

// collectOrphans removes blobs no reference record mentions at all.
//
// These come from a crash in the window between a blob becoming
// durable and its journal record landing. The age check is what makes
// this safe: a blob written seconds ago may simply not be referenced
// YET, and the grace period is far longer than that window.
func (s *Store) collectOrphans(now time.Time, grace time.Duration, st *GCStats) error {
	type victim struct {
		hash string
		size int64
	}
	var victims []victim

	// One read transaction wraps the whole walk so the lookup per blob
	// costs nothing, and memory stays bounded by the number of actual
	// orphans rather than the number of blobs.
	if err := s.idx.View(func(tx *index.Tx) error {
		return s.blobs.Walk(func(hash string, size int64, mod time.Time, bad bool) error {
			if bad {
				st.Foreign++
				return nil
			}
			if now.Sub(mod) < grace {
				return nil
			}
			_, ok, err := tx.RefOf(hash)
			if err != nil || ok {
				return err
			}
			victims = append(victims, victim{hash: hash, size: size})
			return nil
		})
	}); err != nil {
		return err
	}

	for _, v := range victims {
		if err := s.blobs.Remove(v.hash); err != nil {
			return fmt.Errorf("store: gc remove orphan %s: %w", v.hash, err)
		}
		st.Orphans++
		st.BytesReclaimed += v.size
	}
	if len(victims) > 0 {
		s.svcLog.Warn("collected orphan blobs: they were written but never referenced, "+
			"which normally means a crash between the two",
			"count", len(victims))
	}
	return nil
}

// StartGC runs a collection pass on the configured interval until stop
// is closed.
//
// The journal is deliberately NOT pruned here. It is the only durable
// record of which key holds which content — the index is derived from
// it — so trimming it would trade a bounded amount of disk for the
// ability to rebuild after losing the index. Pruning becomes safe once
// secondaries report how far they have consumed (M6).
func (s *Store) StartGC(stop <-chan struct{}) {
	s.mu.RLock()
	interval := s.gc.Interval.Std()
	s.mu.RUnlock()
	if interval <= 0 {
		return
	}

	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				if _, err := s.GC(time.Now()); err != nil {
					s.svcLog.Error("gc pass failed", "error", err)
				}
			}
		}
	}()
}
