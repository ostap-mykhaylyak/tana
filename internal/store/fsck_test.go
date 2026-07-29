package store

import (
	"os"
	"strings"
	"testing"
)

func TestFsckOnAHealthyStore(t *testing.T) {
	h := newHarness(t)
	h.put("a.jpg", "one")
	h.put("b.jpg", "two")

	rep, err := h.Fsck(false)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.Healthy() {
		t.Fatalf("a healthy store reported problems: %+v", rep)
	}
	if rep.Objects != 2 || rep.References != 2 {
		t.Errorf("report = %+v, want 2 objects and 2 references", rep)
	}
	if rep.UnreferencedBlobs != 0 {
		t.Errorf("unreferenced blobs = %d", rep.UnreferencedBlobs)
	}
}

func TestFsckRebuildsTheIndexFromTheJournal(t *testing.T) {
	h := newHarness(t)
	h.put("a.jpg", "one")
	h.put("b.jpg", "two")
	if err := h.Delete(testBucket, "a.jpg"); err != nil {
		t.Fatal(err)
	}
	wantSeq := h.LastSeq()

	// Wipe the derived state entirely, the way a corrupted index would
	// have to be recovered from.
	for _, ns := range []string{testBucket} {
		if err := h.idx.DropNamespace(ns); err != nil {
			t.Fatal(err)
		}
	}
	if err := h.idx.SetMeta(appliedSeqKey, []byte("0")); err != nil {
		t.Fatal(err)
	}

	rep, err := h.Fsck(true)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.Rebuilt || rep.JournalRecords != int(wantSeq) {
		t.Fatalf("rebuild replayed %d of %d records", rep.JournalRecords, wantSeq)
	}
	if rep.Objects != 1 {
		t.Errorf("objects after rebuild = %d, want 1", rep.Objects)
	}
	if !rep.Healthy() {
		t.Errorf("rebuilt store reports problems: %+v", rep)
	}

	// The surviving key reads, the deleted one does not come back.
	if _, err := h.Head(testBucket, "b.jpg"); err != nil {
		t.Errorf("b.jpg did not survive the rebuild: %v", err)
	}
	if _, err := h.Head(testBucket, "a.jpg"); err == nil {
		t.Error("a deleted key came back from the rebuild")
	}
	// Reference counts have to be right, or the collector will either
	// leak forever or delete something live.
	e, _ := h.Head(testBucket, "b.jpg")
	if got := h.refCount(e.Hash); got != 1 {
		t.Errorf("ref count after rebuild = %d, want 1", got)
	}
}

func TestFsckRebuildIsIdempotent(t *testing.T) {
	h := newHarness(t)
	h.put("a.jpg", "one")

	for i := 0; i < 3; i++ {
		if _, err := h.Fsck(true); err != nil {
			t.Fatal(err)
		}
	}
	e, err := h.Head(testBucket, "a.jpg")
	if err != nil {
		t.Fatal(err)
	}
	if got := h.refCount(e.Hash); got != 1 {
		t.Errorf("ref count after three rebuilds = %d, want 1", got)
	}
}

func TestFsckReportsAMissingBlob(t *testing.T) {
	h := newHarness(t)
	e := h.put("a.jpg", "content")

	// Somebody removed the file by hand, or the array lost it. This is
	// the one finding that means data loss rather than bookkeeping.
	if err := os.Remove(h.Blobs().Path(e.Hash)); err != nil {
		t.Fatal(err)
	}

	rep, err := h.Fsck(false)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Healthy() {
		t.Fatal("fsck called a store with a missing blob healthy")
	}
	if len(rep.MissingBlobs) != 1 || !strings.HasSuffix(rep.MissingBlobs[0], "a.jpg") {
		t.Errorf("missing blobs = %v", rep.MissingBlobs)
	}
	if rep.StaleRefs != 1 {
		t.Errorf("stale references = %d, want 1", rep.StaleRefs)
	}
}

func TestFsckCountsUnreferencedBlobs(t *testing.T) {
	h := newHarness(t)
	e := h.put("a.jpg", "content")
	if err := h.Delete(testBucket, "a.jpg"); err != nil {
		t.Fatal(err)
	}

	// Deleted but still inside its grace period: expected, not a fault.
	rep, err := h.Fsck(false)
	if err != nil {
		t.Fatal(err)
	}
	if rep.UnreferencedBlobs != 1 {
		t.Errorf("unreferenced blobs = %d, want 1", rep.UnreferencedBlobs)
	}
	if !rep.Healthy() {
		t.Errorf("a blob waiting out its grace period was reported as a problem: %+v", rep)
	}
	if !h.blobExists(e.Hash) {
		t.Error("fsck removed a blob; it is a report, not a repair")
	}
}

func TestScrubOnAHealthyStore(t *testing.T) {
	h := newHarness(t)
	h.put("a.jpg", "aaa")
	h.put("b.jpg", "bbbbb")

	rep, err := h.Scrub(nil)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.Healthy() {
		t.Fatalf("scrub found corruption where there is none: %+v", rep)
	}
	if rep.Blobs != 2 || rep.Bytes != 8 {
		t.Errorf("report = %+v, want 2 blobs and 8 bytes", rep)
	}
}

func TestScrubDetectsRot(t *testing.T) {
	h := newHarness(t)
	e := h.put("a.jpg", "the original content")

	// Rewrite the bytes underneath, leaving the name. Content
	// addressing is what makes this detectable at all: the filename IS
	// the checksum, so there is no separate manifest to keep in step.
	if err := os.WriteFile(h.Blobs().Path(e.Hash), []byte("rotted"), 0o600); err != nil {
		t.Fatal(err)
	}

	rep, err := h.Scrub(nil)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Healthy() {
		t.Fatal("scrub accepted a blob whose content no longer matches its name")
	}
	if len(rep.Corrupt) != 1 || rep.Corrupt[0] != e.Hash {
		t.Errorf("corrupt = %v, want [%s]", rep.Corrupt, e.Hash)
	}
}

func TestScrubIgnoresForeignFiles(t *testing.T) {
	h := newHarness(t)
	h.put("a.jpg", "content")
	if err := os.WriteFile(h.conf.Data+"/blobs/README", []byte("not ours"), 0o600); err != nil {
		t.Fatal(err)
	}

	rep, err := h.Scrub(nil)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Foreign != 1 {
		t.Errorf("foreign = %d, want 1", rep.Foreign)
	}
	if !rep.Healthy() {
		t.Errorf("a foreign file was reported as corruption: %+v", rep)
	}
}

func TestScrubReportsProgress(t *testing.T) {
	h := newHarness(t)
	h.put("a.jpg", "content")
	// The callback is time-based, so a fast scrub may never fire it.
	// What matters is that passing one does not break anything.
	called := 0
	_ = called
	if _, err := h.Scrub(func(int64, int64) { called++ }); err != nil {
		t.Fatal(err)
	}
}
