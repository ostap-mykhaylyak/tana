package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/ostap-mykhaylyak/tana/internal/config"
	"github.com/ostap-mykhaylyak/tana/internal/index"
)

// Eviction and recall: the two halves of treating local disk as a
// cache rather than as the place the files live.
//
// What makes this safe — and what separates it from mounting a bucket
// over the uploads directory — is that eviction removes only the
// BYTES. The index keeps the size, the modification time and the mode,
// so every question WordPress asks about an evicted object is answered
// locally and instantly. A plugin that stats ten thousand files never
// touches the network. Only a genuine read does, and on a LAN that
// costs single-digit milliseconds.

const (
	// recallTimeout bounds one recall. A read that blocks forever is
	// worse than a read that fails: PHP-FPM workers are a fixed pool,
	// and one stuck request at a time will exhaust it.
	recallTimeout = 2 * time.Minute
	// tempSuffix marks a partially recalled file. Recall writes to it
	// and renames, so a reader never observes a half-filled file at the
	// real path.
	tempSuffix = ".tana-recall"
)

// ErrEvicted reports that an object's bytes are not local. Callers
// that can block should call Recall; callers that cannot should fail.
type ErrEvicted struct{ Key string }

func (e ErrEvicted) Error() string { return "object is evicted: " + e.Key }

// recalls deduplicates concurrent recalls of one key.
type recalls struct {
	mu sync.Mutex
	m  map[string]*sync.WaitGroup
}

// Recall fetches an evicted object back into the backing store.
//
// Concurrent recalls of the same key collapse into one download: a
// page with twenty images on a cold cache would otherwise open twenty
// connections for the same file, and the thundering herd is worst
// exactly when the cache is coldest.
func (a *Agent) Recall(ctx context.Context, key string) error {
	e, ok, err := a.idx.Get(a.site.Name, key)
	if err != nil {
		return err
	}
	if !ok {
		return fs.ErrNotExist
	}
	if e.State.Local() {
		if _, err := os.Stat(a.pathOf(key)); err == nil {
			return nil // already here
		}
	}

	a.recalling.mu.Lock()
	if wg, inflight := a.recalling.m[key]; inflight {
		a.recalling.mu.Unlock()
		wg.Wait()
		// The recall that was already running either succeeded or did
		// not; either way the answer is now on disk or not.
		if _, err := os.Stat(a.pathOf(key)); err == nil {
			return nil
		}
		return fmt.Errorf("agent: recall of %s failed in another request", key)
	}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	if a.recalling.m == nil {
		a.recalling.m = map[string]*sync.WaitGroup{}
	}
	a.recalling.m[key] = wg
	a.recalling.mu.Unlock()

	defer func() {
		a.recalling.mu.Lock()
		delete(a.recalling.m, key)
		a.recalling.mu.Unlock()
		wg.Done()
	}()

	return a.fetch(ctx, key, e)
}

// fetch downloads one object into the backing store.
func (a *Agent) fetch(ctx context.Context, key string, e index.Entry) error {
	start := time.Now()
	ctx, cancel := context.WithTimeout(ctx, recallTimeout)
	defer cancel()

	rc, _, err := a.cli.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("agent: recall %s: %w", key, err)
	}
	defer rc.Close()

	dst := a.pathOf(key)
	if err := os.MkdirAll(filepath.Dir(dst), 0o750); err != nil {
		return err
	}
	tmp := dst + tempSuffix
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, fs.FileMode(e.Mode)&0o777)
	if err != nil {
		return err
	}
	n, err := io.Copy(f, rc)
	if err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("agent: recall %s: %w", key, err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	// Restore the modification time the index recorded. WordPress and
	// several plugins compare it against the database, and a file that
	// comes back from the store looking newer than it is triggers work
	// that has no reason to happen.
	if err := os.Chtimes(tmp, time.Now(), e.ModTime); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, dst); err != nil {
		os.Remove(tmp)
		return err
	}

	if _, err := a.idx.SetState(a.site.Name, key, index.Synced); err != nil {
		return err
	}
	a.idx.Touch(a.site.Name, key, time.Now())
	a.xferLog.Info("recalled", "site", a.site.Name, "key", key,
		"size", n, "duration_ms", time.Since(start).Milliseconds())
	return nil
}

// Evict removes an object's local bytes, keeping its metadata.
//
// It refuses anything not known to be on the store: evicting a dirty
// object would be deleting the only copy.
func (a *Agent) Evict(key string) error {
	e, ok, err := a.idx.Get(a.site.Name, key)
	if err != nil {
		return err
	}
	if !ok {
		return fs.ErrNotExist
	}
	if !e.State.Safe() {
		return fmt.Errorf("agent: refusing to evict %s: it is not on the store yet", key)
	}
	if e.State == index.Evicted {
		return nil
	}
	if err := os.Remove(a.pathOf(key)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	if _, err := a.idx.SetState(a.site.Name, key, index.Evicted); err != nil {
		return err
	}
	a.xferLog.Info("evicted", "site", a.site.Name, "key", key, "size", e.Size)
	return nil
}

// Pin marks an object as never evictable, or releases it.
func (a *Agent) Pin(key string, pinned bool) error {
	return a.idx.Update(func(tx *index.Tx) error {
		e, ok, err := tx.Get(a.site.Name, key)
		if err != nil || !ok {
			if err == nil {
				err = fs.ErrNotExist
			}
			return err
		}
		e.Pinned = pinned
		return tx.Put(a.site.Name, e)
	})
}

// EvictStats is what one eviction pass did.
type EvictStats struct {
	Candidates int   `json:"candidates"`
	Evicted    int   `json:"evicted"`
	BytesFreed int64 `json:"bytes_freed"`
	// Skipped counts objects that were over the size threshold and cold
	// enough, but protected by a pin or a policy pattern.
	Skipped int `json:"skipped"`
}

// EvictToFit removes cold objects until the site is back under its
// cache ceiling.
//
// Coldest first, and never below the size threshold: thumbnails are
// what plugins stat and read constantly, and evicting them frees
// almost nothing while making every page slower. The bytes worth
// reclaiming are in the originals nobody has opened since 2019.
func (a *Agent) EvictToFit(now time.Time) (EvictStats, error) {
	var st EvictStats
	cache := a.site.Cache
	if cache.MaxSize <= 0 && cache.MinFree <= 0 {
		return st, nil // no ceiling configured: mirror everything
	}

	stats, err := a.idx.Stats(a.site.Name)
	if err != nil {
		return st, err
	}
	target := a.overBy(stats.LocalBytes)
	if target <= 0 {
		return st, nil
	}

	type candidate struct {
		key   string
		size  int64
		atime time.Time
	}
	var pool []candidate
	if err := a.idx.Walk(a.site.Name, func(e index.Entry) error {
		if !e.State.Local() || !e.State.Safe() {
			return nil // already evicted, or not safe to evict
		}
		if e.Size < int64(cache.KeepBelow) {
			return nil
		}
		if e.Pinned || a.protected(e.Key) {
			st.Skipped++
			return nil
		}
		pool = append(pool, candidate{key: e.Key, size: e.Size, atime: e.ATime})
		return nil
	}); err != nil {
		return st, err
	}
	st.Candidates = len(pool)

	// Coldest first. Ties broken by size, largest first, so one pass
	// reclaims the target with the fewest evictions.
	sort.Slice(pool, func(i, j int) bool {
		if !pool[i].atime.Equal(pool[j].atime) {
			return pool[i].atime.Before(pool[j].atime)
		}
		return pool[i].size > pool[j].size
	})

	for _, c := range pool {
		if st.BytesFreed >= target {
			break
		}
		if err := a.Evict(c.key); err != nil {
			a.svcLog.Error("eviction failed", "site", a.site.Name, "key", c.key, "error", err)
			continue
		}
		st.Evicted++
		st.BytesFreed += c.size
	}
	if st.Evicted > 0 {
		a.xferLog.Info("eviction pass", "site", a.site.Name,
			"evicted", st.Evicted, "bytes_freed", st.BytesFreed,
			"candidates", st.Candidates, "protected", st.Skipped)
	}
	return st, nil
}

// overBy returns how many bytes must be freed to satisfy both the
// cache ceiling and the free-space floor.
func (a *Agent) overBy(localBytes int64) int64 {
	cache := a.site.Cache
	var need int64
	if cache.MaxSize > 0 && localBytes > int64(cache.MaxSize) {
		need = localBytes - int64(cache.MaxSize)
	}
	if cache.MinFree > 0 {
		if free, err := freeSpace(a.site.Backing); err == nil && free < int64(cache.MinFree) {
			if short := int64(cache.MinFree) - free; short > need {
				need = short
			}
		}
	}
	return need
}

// protected reports whether a key matches a never_evict pattern.
func (a *Agent) protected(key string) bool {
	return config.MatchAny(a.site.Cache.NeverEvict, key)
}

// StartEviction runs an eviction pass on an interval until stop.
func (a *Agent) StartEviction(stop <-chan struct{}, interval time.Duration) {
	if interval <= 0 {
		interval = time.Minute
	}
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				if _, err := a.EvictToFit(time.Now()); err != nil {
					a.svcLog.Error("eviction pass failed", "site", a.site.Name, "error", err)
				}
			}
		}
	}()
}
