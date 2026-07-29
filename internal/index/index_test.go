package index

import (
	"path/filepath"
	"testing"
	"time"
)

func open(t *testing.T) *DB {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "index.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func entry(key string, size int64, s State) Entry {
	return Entry{Key: key, Size: size, ModTime: time.Now(), Mode: 0o644, ETag: "deadbeef", State: s}
}

func TestPutGetDelete(t *testing.T) {
	db := open(t)
	e := entry("2026/07/foto.jpg", 1024, Synced)
	if err := db.Put("shop", e); err != nil {
		t.Fatal(err)
	}

	got, ok, err := db.Get("shop", e.Key)
	if err != nil || !ok {
		t.Fatalf("Get: ok=%v err=%v", ok, err)
	}
	if got.Size != 1024 || got.State != Synced || got.ETag != "deadbeef" {
		t.Errorf("round trip lost data: %+v", got)
	}

	if err := db.Delete("shop", e.Key); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := db.Get("shop", e.Key); ok {
		t.Error("entry survived Delete")
	}
	// Deleting what is not there is how reconciliation calls it.
	if err := db.Delete("shop", "missing"); err != nil {
		t.Errorf("Delete of a missing key: %v", err)
	}
}

func TestGetMissingNamespace(t *testing.T) {
	db := open(t)
	if _, ok, err := db.Get("nope", "k"); ok || err != nil {
		t.Errorf("ok=%v err=%v, want false/nil", ok, err)
	}
}

func TestStatsFollowState(t *testing.T) {
	db := open(t)
	// One evicted object: counted in Bytes, absent from LocalBytes.
	if err := db.Put("shop", entry("a.jpg", 100, Synced)); err != nil {
		t.Fatal(err)
	}
	if err := db.Put("shop", entry("b.jpg", 200, Evicted)); err != nil {
		t.Fatal(err)
	}
	if err := db.Put("shop", entry("c.jpg", 400, Dirty)); err != nil {
		t.Fatal(err)
	}

	st, err := db.Stats("shop")
	if err != nil {
		t.Fatal(err)
	}
	want := Stats{Objects: 3, Bytes: 700, LocalObjects: 2, LocalBytes: 500, DirtyObjects: 1, DirtyBytes: 400}
	if st != want {
		t.Fatalf("stats = %+v, want %+v", st, want)
	}

	// The dirty object completes its upload: it leaves the dirty
	// counters, and stays local because it was local all along.
	if ok, err := db.SetState("shop", "c.jpg", Synced); err != nil || !ok {
		t.Fatalf("SetState: ok=%v err=%v", ok, err)
	}
	st, _ = db.Stats("shop")
	if st.DirtyObjects != 0 || st.DirtyBytes != 0 {
		t.Errorf("dirty counters after sync: %+v", st)
	}
	if st.LocalObjects != 2 || st.LocalBytes != 500 {
		t.Errorf("local counters after sync = %d obj / %d bytes, want 2 / 500", st.LocalObjects, st.LocalBytes)
	}

	// Then it is evicted: still an object, no longer local.
	if _, err := db.SetState("shop", "c.jpg", Evicted); err != nil {
		t.Fatal(err)
	}
	st, _ = db.Stats("shop")
	if st.Objects != 3 || st.LocalObjects != 1 || st.LocalBytes != 100 {
		t.Errorf("stats after eviction: %+v", st)
	}
}

func TestStatsSurviveOverwrite(t *testing.T) {
	// WordPress does not overwrite uploads, but reconciliation
	// rewrites entries constantly; the counters must not double.
	db := open(t)
	for i := 0; i < 5; i++ {
		if err := db.Put("shop", entry("a.jpg", 100, Synced)); err != nil {
			t.Fatal(err)
		}
	}
	st, _ := db.Stats("shop")
	if st.Objects != 1 || st.Bytes != 100 {
		t.Fatalf("stats = %+v, want a single 100 byte object", st)
	}
	// A rewrite with a new size must move the counters, not add to them.
	if err := db.Put("shop", entry("a.jpg", 250, Synced)); err != nil {
		t.Fatal(err)
	}
	st, _ = db.Stats("shop")
	if st.Objects != 1 || st.Bytes != 250 || st.LocalBytes != 250 {
		t.Fatalf("stats = %+v, want a single 250 byte object", st)
	}
}

func TestSetStateUnknownKey(t *testing.T) {
	db := open(t)
	ok, err := db.SetState("shop", "missing", Synced)
	if err != nil || ok {
		t.Errorf("ok=%v err=%v, want false/nil", ok, err)
	}
}

func TestNamespacesAreIsolated(t *testing.T) {
	db := open(t)
	if err := db.Put("a", entry("k", 10, Synced)); err != nil {
		t.Fatal(err)
	}
	if err := db.Put("b", entry("k", 20, Synced)); err != nil {
		t.Fatal(err)
	}
	got, _, _ := db.Get("a", "k")
	if got.Size != 10 {
		t.Errorf("namespace a leaked: %+v", got)
	}
	names, err := db.Namespaces()
	if err != nil || len(names) != 2 {
		t.Fatalf("namespaces = %v, err = %v", names, err)
	}

	if err := db.DropNamespace("a"); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := db.Get("a", "k"); ok {
		t.Error("namespace survived DropNamespace")
	}
	if st, _ := db.Stats("a"); st.Objects != 0 {
		t.Errorf("stats survived DropNamespace: %+v", st)
	}
	if got, _, _ := db.Get("b", "k"); got.Size != 20 {
		t.Error("DropNamespace took the wrong namespace with it")
	}
}

func TestWalkPrefixIsOrdered(t *testing.T) {
	db := open(t)
	keys := []string{"2025/12/z.jpg", "2026/07/a.jpg", "2026/07/b.jpg", "2026/08/c.jpg"}
	for _, k := range keys {
		if err := db.Put("shop", entry(k, 1, Synced)); err != nil {
			t.Fatal(err)
		}
	}
	var got []string
	if err := db.WalkPrefix("shop", "2026/07/", func(e Entry) error {
		got = append(got, e.Key)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != "2026/07/a.jpg" || got[1] != "2026/07/b.jpg" {
		t.Fatalf("WalkPrefix = %v", got)
	}

	var all []string
	if err := db.Walk("shop", func(e Entry) error {
		all = append(all, e.Key)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for i, k := range keys {
		if all[i] != k {
			t.Fatalf("Walk is not in key order: %v", all)
		}
	}
}

func TestTouchIsVisibleBeforeFlush(t *testing.T) {
	db := open(t)
	e := entry("a.jpg", 100, Synced)
	e.ATime = time.Now().Add(-time.Hour)
	if err := db.Put("shop", e); err != nil {
		t.Fatal(err)
	}

	at := time.Now()
	db.Touch("shop", "a.jpg", at)

	// Reads happen far more often than the flush interval, so a
	// pending access time must already be part of what Get returns.
	got, _, _ := db.Get("shop", "a.jpg")
	if !got.ATime.Equal(at) {
		t.Errorf("ATime = %v, want the pending value %v", got.ATime, at)
	}

	db.flushATimes()
	got, _, _ = db.Get("shop", "a.jpg")
	if !got.ATime.Equal(at) {
		t.Errorf("ATime after flush = %v, want %v", got.ATime, at)
	}
}

func TestTouchOfMissingKeyIsHarmless(t *testing.T) {
	db := open(t)
	db.Touch("shop", "gone.jpg", time.Now())
	db.flushATimes() // must not panic or create the entry
	if _, ok, _ := db.Get("shop", "gone.jpg"); ok {
		t.Error("Touch created an entry")
	}
}

func TestReopenKeepsData(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "index.db")

	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put("shop", entry("a.jpg", 100, Evicted)); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	got, ok, _ := db.Get("shop", "a.jpg")
	if !ok || got.State != Evicted {
		t.Fatalf("entry did not survive a restart: %+v ok=%v", got, ok)
	}
	st, _ := db.Stats("shop")
	if st.Objects != 1 || st.LocalObjects != 0 {
		t.Errorf("stats did not survive a restart: %+v", st)
	}
}

func TestPutRejectsEmptyKey(t *testing.T) {
	db := open(t)
	if err := db.Put("shop", Entry{}); err == nil {
		t.Error("accepted an entry with no key")
	}
}

func TestStateHelpers(t *testing.T) {
	if !Synced.Local() || Evicted.Local() {
		t.Error("Local is wrong")
	}
	if Dirty.Safe() || !Evicted.Safe() || !Synced.Safe() {
		t.Error("Safe is wrong")
	}
	for _, s := range []State{Dirty, Uploading, Synced, Evicted} {
		got, ok := ParseState(s.String())
		if !ok || got != s {
			t.Errorf("ParseState(%q) = %v, %v", s.String(), got, ok)
		}
	}
}
