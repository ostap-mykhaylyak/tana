package journal

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func open(t *testing.T, dir string) *Journal {
	t.Helper()
	j, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { j.Close() })
	return j
}

func appendN(t *testing.T, j *Journal, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, err := j.Append(Record{Op: OpPut, Bucket: "b", Key: "k", Hash: "h", Size: int64(i)}); err != nil {
			t.Fatal(err)
		}
	}
}

func collect(t *testing.T, j *Journal, from uint64) []Record {
	t.Helper()
	var out []Record
	if err := j.Replay(from, func(r Record) error {
		out = append(out, r)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestAppendAssignsSequentialSeq(t *testing.T) {
	j := open(t, t.TempDir())
	for i := uint64(1); i <= 5; i++ {
		seq, err := j.Append(Record{Op: OpPut, Key: "k"})
		if err != nil {
			t.Fatal(err)
		}
		if seq != i {
			t.Fatalf("seq = %d, want %d", seq, i)
		}
	}
	if j.LastSeq() != 5 {
		t.Errorf("LastSeq = %d, want 5", j.LastSeq())
	}
}

func TestReplayRoundTrip(t *testing.T) {
	j := open(t, t.TempDir())
	if _, err := j.Append(Record{Op: OpPut, Bucket: "shop", Key: "a.jpg", Hash: "abc", Size: 42}); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Append(Record{Op: OpDelete, Bucket: "shop", Key: "a.jpg"}); err != nil {
		t.Fatal(err)
	}

	got := collect(t, j, 0)
	if len(got) != 2 {
		t.Fatalf("replayed %d records", len(got))
	}
	if got[0].Op != OpPut || got[0].Bucket != "shop" || got[0].Key != "a.jpg" || got[0].Size != 42 {
		t.Errorf("first record lost data: %+v", got[0])
	}
	if got[1].Op != OpDelete {
		t.Errorf("second record = %+v", got[1])
	}
	if got[0].Time.IsZero() {
		t.Error("Append did not stamp a time")
	}
}

func TestReplayFromSkipsApplied(t *testing.T) {
	j := open(t, t.TempDir())
	appendN(t, j, 10)

	got := collect(t, j, 8)
	if len(got) != 3 {
		t.Fatalf("replayed %d records from seq 8, want 3", len(got))
	}
	if got[0].Seq != 8 || got[2].Seq != 10 {
		t.Errorf("replayed seqs %d..%d", got[0].Seq, got[2].Seq)
	}
}

func TestReopenRecoversLastSeq(t *testing.T) {
	dir := t.TempDir()
	j := open(t, dir)
	appendN(t, j, 4)
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}

	j2 := open(t, dir)
	if j2.LastSeq() != 4 {
		t.Fatalf("LastSeq after reopen = %d, want 4", j2.LastSeq())
	}
	seq, err := j2.Append(Record{Op: OpPut, Key: "k"})
	if err != nil {
		t.Fatal(err)
	}
	if seq != 5 {
		t.Errorf("seq after reopen = %d, want 5 (numbering must not restart)", seq)
	}
	if got := collect(t, j2, 0); len(got) != 5 {
		t.Errorf("replayed %d records after reopen", len(got))
	}
}

func TestTornTailIsTruncated(t *testing.T) {
	dir := t.TempDir()
	j := open(t, dir)
	appendN(t, j, 3)
	path := filepath.Join(dir, segmentName(1))
	j.Close()

	// Simulate a crash mid-append: a partial line with no newline.
	// No caller was ever told this record succeeded.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`{"seq":4,"op":"put","key":"tru`); err != nil {
		t.Fatal(err)
	}
	f.Close()

	j2 := open(t, dir)
	if j2.LastSeq() != 3 {
		t.Fatalf("LastSeq = %d, want 3: the torn record must not count", j2.LastSeq())
	}
	if got := collect(t, j2, 0); len(got) != 3 {
		t.Fatalf("replayed %d records, want 3", len(got))
	}
	// And the next append must reuse seq 4 cleanly.
	seq, err := j2.Append(Record{Op: OpPut, Key: "after"})
	if err != nil {
		t.Fatal(err)
	}
	if seq != 4 {
		t.Errorf("seq = %d, want 4", seq)
	}
	got := collect(t, j2, 0)
	if len(got) != 4 || got[3].Key != "after" {
		t.Errorf("records after recovery: %+v", got)
	}
}

func TestCorruptRecordMidFileIsReported(t *testing.T) {
	dir := t.TempDir()
	j := open(t, dir)
	appendN(t, j, 3)
	path := filepath.Join(dir, segmentName(1))
	j.Close()

	// Garbage followed by a newline is not a torn write: something
	// damaged a record that had already been flushed, and swallowing
	// that would silently lose data.
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.SplitN(string(raw), "\n", 2)
	if err := os.WriteFile(path, []byte("{not json}\n"+lines[1]), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); err == nil {
		t.Fatal("expected an error for a corrupt record in the middle of a segment")
	}
}

func TestRotationAndPrune(t *testing.T) {
	old := maxSegment
	maxSegment = 300 // a few records per segment
	t.Cleanup(func() { maxSegment = old })

	dir := t.TempDir()
	j := open(t, dir)
	appendN(t, j, 20)

	segs, err := j.segments()
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) < 2 {
		t.Fatalf("segments = %d, want the journal to have rotated", len(segs))
	}
	if got := collect(t, j, 0); len(got) != 20 {
		t.Fatalf("replayed %d records across segments, want 20", len(got))
	}

	// Pruning drops whole segments and never rewrites one, so anything
	// at or above keepFrom must survive intact.
	keepFrom := segs[len(segs)-1].base
	removed, err := j.Prune(keepFrom)
	if err != nil {
		t.Fatal(err)
	}
	if removed == 0 {
		t.Fatal("Prune removed nothing")
	}
	got := collect(t, j, keepFrom)
	if len(got) == 0 || got[0].Seq != keepFrom {
		t.Errorf("records from %d did not survive pruning: %d found", keepFrom, len(got))
	}
}

func TestPruneKeepsLiveSegments(t *testing.T) {
	dir := t.TempDir()
	j := open(t, dir)
	appendN(t, j, 5)

	// Everything is in the open segment, which holds live records.
	removed, err := j.Prune(3)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 0 {
		t.Errorf("Prune removed %d segment(s) that still hold live records", removed)
	}
	if got := collect(t, j, 0); len(got) != 5 {
		t.Errorf("replayed %d records after a no-op prune", len(got))
	}
}

func TestEmptyJournal(t *testing.T) {
	j := open(t, t.TempDir())
	if j.LastSeq() != 0 {
		t.Errorf("LastSeq on an empty journal = %d", j.LastSeq())
	}
	if got := collect(t, j, 0); len(got) != 0 {
		t.Errorf("replayed %d records from an empty journal", len(got))
	}
}

func TestSegmentNameIsLexicallyOrdered(t *testing.T) {
	// A plain `ls` must show segments in sequence order.
	if segmentName(2) >= segmentName(10) {
		t.Errorf("%s sorts after %s", segmentName(2), segmentName(10))
	}
}
