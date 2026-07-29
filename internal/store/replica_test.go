package store

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ostap-mykhaylyak/tana/internal/config"
	"github.com/ostap-mykhaylyak/tana/internal/journal"
)

const testSecret = "a-shared-replication-secret"

// pair is a primary with a secondary pulling from it.
type pair struct {
	primary   *harness
	secondary *harness
	sec       *Secondary
	srv       *httptest.Server
	t         *testing.T
}

func newPair(t *testing.T) *pair {
	t.Helper()
	primary := newHarness(t)
	secondary := newHarness(t)

	srv := httptest.NewServer(primary.ReplicaHandler(testSecret))
	t.Cleanup(srv.Close)

	sec := NewSecondary(secondary.Store, config.Replica{
		Mode: config.ReplicaSecondary, From: srv.URL, Secret: testSecret,
	})
	return &pair{primary: primary, secondary: secondary, sec: sec, srv: srv, t: t}
}

// sync pulls until the secondary has caught up.
func (p *pair) sync() {
	p.t.Helper()
	for i := 0; i < 50; i++ {
		n, err := p.sec.Pull(context.Background())
		if err != nil {
			p.t.Fatalf("pull: %v", err)
		}
		if n == 0 {
			return
		}
	}
	p.t.Fatal("replication did not converge")
}

// read returns an object's content from a store.
func read(t *testing.T, h *harness, key string) (string, bool) {
	t.Helper()
	_, rc, err := h.Get(testBucket, key)
	if err != nil {
		return "", false
	}
	defer rc.Close()
	raw, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw), true
}

func TestReplicationCopiesObjects(t *testing.T) {
	p := newPair(t)
	p.primary.put("2026/07/foto.jpg", "the bytes")
	p.primary.put("2026/07/altra.jpg", "other bytes")
	p.sync()

	got, ok := read(t, p.secondary, "2026/07/foto.jpg")
	if !ok {
		t.Fatal("the object never reached the secondary")
	}
	if got != "the bytes" {
		t.Errorf("secondary holds %q", got)
	}

	// The secondary keeps its own journal, so it can be promoted and
	// can rebuild its own index without asking anyone.
	if p.secondary.LastSeq() != p.primary.LastSeq() {
		t.Errorf("journal seq: secondary %d, primary %d",
			p.secondary.LastSeq(), p.primary.LastSeq())
	}
	applied, err := p.secondary.AppliedSeq()
	if err != nil {
		t.Fatal(err)
	}
	if applied != p.primary.LastSeq() {
		t.Errorf("secondary applied %d of %d", applied, p.primary.LastSeq())
	}
}

func TestReplicationCopiesDeletes(t *testing.T) {
	p := newPair(t)
	p.primary.put("a.jpg", "doomed")
	p.sync()
	if _, ok := read(t, p.secondary, "a.jpg"); !ok {
		t.Fatal("setup: object not replicated")
	}

	if err := p.primary.Delete(testBucket, "a.jpg"); err != nil {
		t.Fatal(err)
	}
	p.sync()

	if _, ok := read(t, p.secondary, "a.jpg"); ok {
		t.Error("the deletion was not replicated")
	}
}

func TestReplicationDedupesBlobs(t *testing.T) {
	p := newPair(t)
	// Two keys, one content: the secondary must fetch the bytes once.
	p.primary.put("a.jpg", "identical")
	p.primary.put("b.jpg", "identical")
	p.sync()

	count, _, err := p.secondary.Blobs().Usage()
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("secondary holds %d blobs, want 1", count)
	}
}

func TestReplicationResumesAfterDowntime(t *testing.T) {
	p := newPair(t)
	p.primary.put("first.jpg", "before")
	p.sync()

	// The secondary is "down": several writes happen with no pulling.
	for i := 0; i < 5; i++ {
		p.primary.put(string(rune('a'+i))+".jpg", "while away")
	}
	p.sync()

	for i := 0; i < 5; i++ {
		key := string(rune('a'+i)) + ".jpg"
		if _, ok := read(t, p.secondary, key); !ok {
			t.Errorf("%s was missed while the secondary was away", key)
		}
	}
}

func TestReplicationIsIncremental(t *testing.T) {
	p := newPair(t)
	p.primary.put("a.jpg", "one")
	p.sync()

	// A second pull with nothing new must do nothing rather than
	// replaying from the beginning.
	n, err := p.sec.Pull(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("pulled %d records when nothing had changed", n)
	}

	p.primary.put("b.jpg", "two")
	n, err = p.sec.Pull(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("pulled %d records for one new object", n)
	}
}

func TestReplicationRefusesWithoutTheSecret(t *testing.T) {
	p := newPair(t)
	p.primary.put("a.jpg", "private")

	for _, header := range []string{"", "TANA-REPLICA wrong-secret", "Bearer " + testSecret} {
		req, err := http.NewRequest(http.MethodGet, p.srv.URL+"/-/replica/journal?from=1", nil)
		if err != nil {
			t.Fatal(err)
		}
		if header != "" {
			req.Header.Set("Authorization", header)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("Authorization %q got status %d, want 403", header, resp.StatusCode)
		}
	}
}

func TestReplicaEndpointRefusesMalformedHash(t *testing.T) {
	p := newPair(t)
	// The endpoint takes a hash straight from the URL, so it is a place
	// a path could escape the blob store if it were not checked.
	req, err := http.NewRequest(http.MethodGet, p.srv.URL+"/-/replica/blob/notahash", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "TANA-REPLICA "+testSecret)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

func TestSecondaryRejectsAGapInTheSequence(t *testing.T) {
	p := newPair(t)
	p.primary.put("a.jpg", "one")
	p.sync()

	// A record that skips ahead must be refused: replication that can
	// skip is replication that silently diverges.
	err := p.secondary.Journal().AppendAt(journalRecordAt(p.secondary.LastSeq() + 5))
	if err == nil {
		t.Fatal("a record with a gap in the sequence was accepted")
	}
	if !strings.Contains(err.Error(), "out of order") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestReplicationStatusReportsLag(t *testing.T) {
	p := newPair(t)
	p.primary.put("a.jpg", "one")
	p.sync()

	st := p.sec.Status()
	if st.Lag != 0 {
		t.Errorf("lag = %d after syncing, want 0", st.Lag)
	}
	if st.Error != "" {
		t.Errorf("status reports an error after a clean sync: %s", st.Error)
	}
	if st.LastPull.IsZero() {
		t.Error("status does not record when the last pull happened")
	}

	p.primary.put("b.jpg", "two")
	p.primary.put("c.jpg", "three")
	if _, err := p.sec.Pull(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := p.sec.Status(); got.Applied != p.primary.LastSeq() {
		t.Errorf("applied %d of %d", got.Applied, p.primary.LastSeq())
	}
}

func TestSecondaryReportsAnUnreachablePrimary(t *testing.T) {
	p := newPair(t)
	p.srv.Close()

	if _, err := p.sec.Pull(context.Background()); err == nil {
		t.Fatal("pulling from a dead primary succeeded")
	}
	if p.sec.Status().Error == "" {
		t.Error("status does not report the failure")
	}
}

func TestSecondaryDetectsATimeoutFreely(t *testing.T) {
	p := newPair(t)
	p.primary.put("a.jpg", "one")

	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	if _, err := p.sec.Pull(ctx); err == nil {
		t.Error("an expired context did not stop the pull")
	}
}

// journalRecordAt builds a put record with an explicit sequence, for
// the gap check.
func journalRecordAt(seq uint64) journal.Record {
	return journal.Record{Seq: seq, Op: journal.OpPut, Bucket: testBucket, Key: "x", Hash: "h"}
}
