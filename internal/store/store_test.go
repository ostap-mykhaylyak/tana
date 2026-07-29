package store

import (
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ostap-mykhaylyak/tana/internal/config"
	"github.com/ostap-mykhaylyak/tana/internal/index"
	"github.com/ostap-mykhaylyak/tana/internal/journal"
)

const testBucket = "shop-uploads"

func discard() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// harness is a store on a temp dir, with its index, ready to use.
type harness struct {
	*Store
	idx  *index.DB
	dir  string
	tb   testing.TB
	conf config.Store
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	dir := t.TempDir()
	return openHarness(t, dir)
}

func openHarness(t *testing.T, dir string) *harness {
	t.Helper()
	idx, err := index.Open(filepath.Join(dir, "index.db"))
	if err != nil {
		t.Fatal(err)
	}
	conf := config.Store{
		Data:    filepath.Join(dir, "data"),
		Buckets: []config.Bucket{{Name: testBucket, AccessKey: "AK", SecretKey: "SK"}},
		GC:      config.GC{Interval: config.Duration(time.Hour), Grace: config.Duration(72 * time.Hour)},
	}
	s, err := New(conf, idx, discard(), discard())
	if err != nil {
		idx.Close()
		t.Fatal(err)
	}
	h := &harness{Store: s, idx: idx, dir: dir, tb: t, conf: conf}
	t.Cleanup(func() { s.Close(); idx.Close() })
	return h
}

// close releases the harness so the same directory can be reopened.
func (h *harness) close() {
	h.tb.Helper()
	if err := h.Store.Close(); err != nil {
		h.tb.Fatal(err)
	}
	if err := h.idx.Close(); err != nil {
		h.tb.Fatal(err)
	}
}

func (h *harness) put(key, content string) index.Entry {
	h.tb.Helper()
	e, err := h.Put(testBucket, key, strings.NewReader(content), time.Now().UTC())
	if err != nil {
		h.tb.Fatal(err)
	}
	return e
}

// refCount reads a blob's reference count.
func (h *harness) refCount(hash string) int64 {
	h.tb.Helper()
	var out int64
	if err := h.idx.View(func(tx *index.Tx) error {
		r, ok, err := tx.RefOf(hash)
		if err != nil || !ok {
			out = -1
			return err
		}
		out = r.Count
		return nil
	}); err != nil {
		h.tb.Fatal(err)
	}
	return out
}

// blobExists reports whether a blob is still on disk.
func (h *harness) blobExists(hash string) bool {
	h.tb.Helper()
	_, ok, err := h.Blobs().Stat(hash)
	if err != nil {
		h.tb.Fatal(err)
	}
	return ok
}

func TestPutHeadGet(t *testing.T) {
	h := newHarness(t)
	const content = "a photo, pretend"
	e := h.put("2026/07/foto.jpg", content)

	if e.Size != int64(len(content)) || e.State != index.Synced {
		t.Fatalf("entry = %+v", e)
	}

	head, err := h.Head(testBucket, "2026/07/foto.jpg")
	if err != nil {
		t.Fatal(err)
	}
	if head.Hash != e.Hash || head.Size != e.Size {
		t.Errorf("Head disagrees with Put: %+v vs %+v", head, e)
	}

	_, rc, err := h.Get(testBucket, "2026/07/foto.jpg")
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != content {
		t.Errorf("read back %q", got)
	}
}

func TestUnknownBucketAndKey(t *testing.T) {
	h := newHarness(t)
	var nb ErrNoSuchBucket
	if _, err := h.Head("nope", "k"); !errors.As(err, &nb) {
		t.Errorf("Head on an unknown bucket: %v", err)
	}
	if _, err := h.Put("nope", "k", strings.NewReader("x"), time.Now()); !errors.As(err, &nb) {
		t.Errorf("Put on an unknown bucket: %v", err)
	}
	var nk ErrNoSuchKey
	if _, err := h.Head(testBucket, "missing"); !errors.As(err, &nk) {
		t.Errorf("Head on an unknown key: %v", err)
	}
	// S3 deletes are idempotent.
	if err := h.Delete(testBucket, "missing"); err != nil {
		t.Errorf("Delete of a missing key: %v", err)
	}
}

func TestDedupSharesOneBlob(t *testing.T) {
	h := newHarness(t)
	// The same image uploaded to two paths — routine on a WordPress
	// install, and the reason content addressing pays for itself.
	a := h.put("2026/07/a.jpg", "identical bytes")
	b := h.put("2026/07/b.jpg", "identical bytes")

	if a.Hash != b.Hash {
		t.Fatal("identical content produced different hashes")
	}
	if got := h.refCount(a.Hash); got != 2 {
		t.Fatalf("ref count = %d, want 2", got)
	}
	count, _, err := h.Blobs().Usage()
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("blobs on disk = %d, want 1", count)
	}
}

func TestDeleteReleasesOneReference(t *testing.T) {
	h := newHarness(t)
	a := h.put("a.jpg", "shared")
	h.put("b.jpg", "shared")

	if err := h.Delete(testBucket, "a.jpg"); err != nil {
		t.Fatal(err)
	}
	if got := h.refCount(a.Hash); got != 1 {
		t.Fatalf("ref count = %d, want 1", got)
	}
	// The surviving key must still read: this is the failure mode
	// refcounting exists to prevent.
	if !h.blobExists(a.Hash) {
		t.Fatal("the blob was removed while another key still points at it")
	}
	if _, err := h.Head(testBucket, "b.jpg"); err != nil {
		t.Errorf("surviving key broke: %v", err)
	}
}

func TestRewritingAKeyWithSameContentDoesNotInflateRefs(t *testing.T) {
	h := newHarness(t)
	e := h.put("a.jpg", "same")
	h.put("a.jpg", "same")
	h.put("a.jpg", "same")

	// If each rewrite incremented the count, the blob would become
	// permanently uncollectable.
	if got := h.refCount(e.Hash); got != 1 {
		t.Fatalf("ref count = %d, want 1", got)
	}
}

func TestOverwritingAKeyMovesTheReference(t *testing.T) {
	h := newHarness(t)
	oldE := h.put("a.jpg", "version one")
	newE := h.put("a.jpg", "version two")

	if oldE.Hash == newE.Hash {
		t.Fatal("different content produced the same hash")
	}
	if got := h.refCount(oldE.Hash); got != 0 {
		t.Errorf("old blob ref count = %d, want 0", got)
	}
	if got := h.refCount(newE.Hash); got != 1 {
		t.Errorf("new blob ref count = %d, want 1", got)
	}
	// Unreferenced is not deleted: the grace period has not run.
	if !h.blobExists(oldE.Hash) {
		t.Error("the old blob was removed before its grace period elapsed")
	}
}

func TestGCRespectsGracePeriod(t *testing.T) {
	h := newHarness(t)
	e := h.put("a.jpg", "doomed")
	if err := h.Delete(testBucket, "a.jpg"); err != nil {
		t.Fatal(err)
	}

	// Immediately after the delete, nothing may be collected: this is
	// the window that makes an accidental mass delete recoverable.
	st, err := h.GC(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if st.Collected != 0 || !h.blobExists(e.Hash) {
		t.Fatalf("collected %d blob(s) inside the grace period", st.Collected)
	}

	// Once it has elapsed, the blob goes.
	st, err = h.GC(time.Now().Add(73 * time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if st.Collected != 1 {
		t.Fatalf("collected %d, want 1", st.Collected)
	}
	if h.blobExists(e.Hash) {
		t.Error("the blob survived collection")
	}
	if st.BytesReclaimed != int64(len("doomed")) {
		t.Errorf("reclaimed %d bytes, want %d", st.BytesReclaimed, len("doomed"))
	}
}

func TestGCSparesBlobsReferencedAgain(t *testing.T) {
	h := newHarness(t)
	e := h.put("a.jpg", "revived")
	if err := h.Delete(testBucket, "a.jpg"); err != nil {
		t.Fatal(err)
	}
	// Somebody uploads the same content again during the grace window.
	h.put("b.jpg", "revived")

	st, err := h.GC(time.Now().Add(73 * time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if st.Collected != 0 {
		t.Errorf("collected %d blob(s) that had been referenced again", st.Collected)
	}
	if !h.blobExists(e.Hash) {
		t.Fatal("a live blob was collected")
	}
}

func TestGCCollectsOrphans(t *testing.T) {
	h := newHarness(t)
	// A blob that reached the disk but whose journal record never
	// landed: exactly what a crash between the two leaves behind.
	info, err := h.Blobs().Put(strings.NewReader("orphaned by a crash"))
	if err != nil {
		t.Fatal(err)
	}
	if got := h.refCount(info.Hash); got != -1 {
		t.Fatalf("the orphan has a ref record (count %d)", got)
	}

	// Young orphans are spared: not referenced YET is not the same as
	// not referenced.
	st, err := h.GC(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if st.Orphans != 0 || !h.blobExists(info.Hash) {
		t.Fatalf("collected a young orphan (%d)", st.Orphans)
	}

	st, err = h.GC(time.Now().Add(73 * time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if st.Orphans != 1 {
		t.Fatalf("orphans collected = %d, want 1", st.Orphans)
	}
	if h.blobExists(info.Hash) {
		t.Error("the orphan survived")
	}
}

func TestGCLeavesForeignFilesAlone(t *testing.T) {
	h := newHarness(t)
	foreign := filepath.Join(h.conf.Data, "blobs", "NOTES.txt")
	if err := os.WriteFile(foreign, []byte("put here by a human"), 0o600); err != nil {
		t.Fatal(err)
	}

	st, err := h.GC(time.Now().Add(73 * time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if st.Foreign != 1 {
		t.Errorf("foreign files counted = %d, want 1", st.Foreign)
	}
	if _, err := os.Stat(foreign); err != nil {
		t.Error("gc removed a file tana did not write")
	}
}

func TestListWalksInKeyOrder(t *testing.T) {
	h := newHarness(t)
	for _, k := range []string{"2026/08/c.jpg", "2026/07/a.jpg", "2025/12/z.jpg", "2026/07/b.jpg"} {
		h.put(k, k)
	}

	var all []string
	if err := h.List(testBucket, "", func(e index.Entry) error {
		all = append(all, e.Key)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	want := []string{"2025/12/z.jpg", "2026/07/a.jpg", "2026/07/b.jpg", "2026/08/c.jpg"}
	for i := range want {
		if all[i] != want[i] {
			t.Fatalf("List = %v, want %v", all, want)
		}
	}

	var pref []string
	if err := h.List(testBucket, "2026/07/", func(e index.Entry) error {
		pref = append(pref, e.Key)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(pref) != 2 {
		t.Errorf("prefix list = %v, want two keys", pref)
	}
}

func TestJournalTracksMutations(t *testing.T) {
	h := newHarness(t)
	h.put("a.jpg", "one")
	if err := h.Delete(testBucket, "a.jpg"); err != nil {
		t.Fatal(err)
	}

	var ops []journal.Op
	if err := h.Journal().Replay(0, func(r journal.Record) error {
		ops = append(ops, r.Op)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(ops) != 2 || ops[0] != journal.OpPut || ops[1] != journal.OpDelete {
		t.Fatalf("journal ops = %v", ops)
	}

	applied, err := h.AppliedSeq()
	if err != nil {
		t.Fatal(err)
	}
	if applied != h.LastSeq() {
		t.Errorf("applied %d, journal at %d: the index must be caught up after a clean write",
			applied, h.LastSeq())
	}
}

func TestRecoverReplaysAnIndexLeftBehind(t *testing.T) {
	dir := t.TempDir()
	h := openHarness(t, dir)
	h.put("a.jpg", "one")
	h.put("b.jpg", "two")
	if err := h.Delete(testBucket, "a.jpg"); err != nil {
		t.Fatal(err)
	}
	wantSeq := h.LastSeq()
	h.close()

	// Simulate a crash after the journal was flushed but before the
	// index committed: drop the index entirely and rewind the applied
	// position. The journal alone must be able to rebuild the mapping.
	if err := os.Remove(filepath.Join(dir, "index.db")); err != nil {
		t.Fatal(err)
	}

	h2 := openHarness(t, dir)
	applied, err := h2.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if applied != int(wantSeq) {
		t.Fatalf("replayed %d records, want %d", applied, wantSeq)
	}

	// b.jpg was written and never deleted: it must be back.
	e, err := h2.Head(testBucket, "b.jpg")
	if err != nil {
		t.Fatalf("b.jpg did not survive recovery: %v", err)
	}
	if e.Size != 3 {
		t.Errorf("recovered entry = %+v", e)
	}
	// a.jpg was deleted: recovery must not resurrect it.
	var nk ErrNoSuchKey
	if _, err := h2.Head(testBucket, "a.jpg"); !errors.As(err, &nk) {
		t.Errorf("a deleted key came back from recovery: %v", err)
	}
	// And the reference counts must be right, or the collector will
	// either leak or delete something live.
	if got := h2.refCount(e.Hash); got != 1 {
		t.Errorf("ref count after recovery = %d, want 1", got)
	}
}

func TestRecoverIsIdempotent(t *testing.T) {
	h := newHarness(t)
	e := h.put("a.jpg", "content")

	// Rewinding the applied position and replaying must reach the same
	// state, not a doubled reference count.
	if err := h.idx.SetMeta(appliedSeqKey, []byte("0")); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Recover(); err != nil {
		t.Fatal(err)
	}
	if got := h.refCount(e.Hash); got != 1 {
		t.Errorf("ref count after replaying an applied record = %d, want 1", got)
	}
	if _, err := h.Head(testBucket, "a.jpg"); err != nil {
		t.Errorf("key lost during replay: %v", err)
	}
}

func TestRecoverIsANoOpWhenCaughtUp(t *testing.T) {
	h := newHarness(t)
	h.put("a.jpg", "content")
	n, err := h.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("replayed %d records on a caught-up store", n)
	}
}

func TestConfigureSwapsBuckets(t *testing.T) {
	h := newHarness(t)
	h.Configure(config.Store{
		Data:    h.conf.Data,
		Buckets: []config.Bucket{{Name: "other-bucket", AccessKey: "AK", SecretKey: "SK"}},
		GC:      h.conf.GC,
	})
	var nb ErrNoSuchBucket
	if _, err := h.Head(testBucket, "k"); !errors.As(err, &nb) {
		t.Errorf("the removed bucket is still served: %v", err)
	}
	if _, ok := h.Bucket("other-bucket"); !ok {
		t.Error("the added bucket is not served")
	}
}

func TestPutRejectsEmptyKey(t *testing.T) {
	h := newHarness(t)
	if _, err := h.Put(testBucket, "", strings.NewReader("x"), time.Now()); err == nil {
		t.Error("accepted an empty key")
	}
}
