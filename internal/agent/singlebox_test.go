package agent

import (
	"context"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ostap-mykhaylyak/tana/internal/config"
	"github.com/ostap-mykhaylyak/tana/internal/index"
	"github.com/ostap-mykhaylyak/tana/internal/s3"
	"github.com/ostap-mykhaylyak/tana/internal/store"
)

// One machine running both roles: a small deployment, or a first test.
//
// The store and the agent then share one index file, and the worst
// case is deliberately reproduced here — the site is named exactly
// like the bucket. Without scoped namespaces the two would write over
// each other's entries, and the store's rebuild would take the site's
// metadata with it.

// singlebox is a store and an agent over ONE index, as the daemon runs
// them when roles is [store, agent].
type singlebox struct {
	agent   *Agent
	store   *store.Store
	idx     *index.DB
	backing string
	stop    chan struct{}
	t       *testing.T
}

// sameName is used for both the bucket and the site on purpose.
const sameName = "shop-uploads"

func newSinglebox(t *testing.T) *singlebox {
	t.Helper()
	dir := t.TempDir()
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))

	// One index file, exactly as cmd/tana opens it.
	idx, err := index.Open(filepath.Join(dir, "index.db"))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.New(config.Store{
		Data:    filepath.Join(dir, "data"),
		Region:  testRegion,
		Buckets: []config.Bucket{{Name: sameName, AccessKey: testAK, SecretKey: testSK}},
		GC:      config.GC{Interval: config.Duration(time.Hour), Grace: config.Duration(72 * time.Hour)},
	}, idx, discard, discard)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(s3.New(st, testRegion, discard, discard))

	backing := filepath.Join(dir, "backing")
	a, err := New(config.Site{
		Name:    sameName, // same as the bucket: the collision case
		Uploads: filepath.Join(dir, "uploads"),
		Backing: backing,
		Backend: config.Backend{
			Endpoint: srv.URL, Bucket: sameName, Region: testRegion,
			AccessKey: testAK, SecretKey: testSK, Ack: config.AckRemote,
		},
	}, idx, discard, discard)
	if err != nil {
		t.Fatal(err)
	}

	stop := make(chan struct{})
	t.Cleanup(func() {
		close(stop)
		a.Wait()
		srv.Close()
		st.Close()
		idx.Close()
	})
	return &singlebox{agent: a, store: st, idx: idx, backing: backing, stop: stop, t: t}
}

func (b *singlebox) write(rel, content string) {
	b.t.Helper()
	p := filepath.Join(b.backing, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
		b.t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o640); err != nil {
		b.t.Fatal(err)
	}
}

func (b *singlebox) settle() {
	b.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := b.agent.Drain(ctx); err != nil {
		b.t.Fatalf("queue did not drain: %v", err)
	}
}

func (b *singlebox) remote(key string) (string, bool) {
	b.t.Helper()
	_, rc, err := b.store.Get(sameName, key)
	if err != nil {
		return "", false
	}
	defer rc.Close()
	raw, err := io.ReadAll(rc)
	if err != nil {
		b.t.Fatal(err)
	}
	return string(raw), true
}

func TestSingleBoxRoundTrip(t *testing.T) {
	b := newSinglebox(t)
	b.write("2026/07/foto.jpg", "a photo on one machine")
	if err := b.agent.Start(b.stop); err != nil {
		t.Fatal(err)
	}
	b.settle()

	if got, ok := b.remote("2026/07/foto.jpg"); !ok || got != "a photo on one machine" {
		t.Fatalf("the object did not reach the store on the same host: %q, %v", got, ok)
	}
}

func TestSingleBoxKeepsTheTwoIndexesApart(t *testing.T) {
	b := newSinglebox(t)
	b.write("2026/07/foto.jpg", "content")
	if err := b.agent.Start(b.stop); err != nil {
		t.Fatal(err)
	}
	b.settle()

	// Both sides know the object, and each holds its own view: the
	// agent tracks where the bytes are, the store tracks which blob
	// they are. Sharing an entry would make one of those views wrong.
	agentStats, err := b.agent.Stats()
	if err != nil {
		t.Fatal(err)
	}
	storeStats, err := b.store.Stats(sameName)
	if err != nil {
		t.Fatal(err)
	}
	if agentStats.Objects != 1 || storeStats.Objects != 1 {
		t.Fatalf("agent has %d objects, store has %d, want 1 each",
			agentStats.Objects, storeStats.Objects)
	}

	// The agent's entry is Synced (bytes here and there); the store's
	// is Synced too but carries the blob hash. If they shared a row,
	// one of the two would have overwritten the other.
	agentEntry, ok, err := b.idx.Get(b.agent.Namespace(), "2026/07/foto.jpg")
	if err != nil || !ok {
		t.Fatal("agent entry missing")
	}
	storeEntry, err := b.store.Head(sameName, "2026/07/foto.jpg")
	if err != nil {
		t.Fatal(err)
	}
	if storeEntry.Hash == "" {
		t.Error("the store entry lost its blob hash")
	}
	if agentEntry.Mode == 0 {
		t.Error("the agent entry lost the file mode it recorded")
	}
}

func TestSingleBoxEvictionDoesNotTouchTheStore(t *testing.T) {
	b := newSinglebox(t)
	b.write("2026/07/foto.jpg", "content that will be evicted")
	if err := b.agent.Start(b.stop); err != nil {
		t.Fatal(err)
	}
	b.settle()

	if err := b.agent.Evict("2026/07/foto.jpg"); err != nil {
		t.Fatal(err)
	}

	// Evicting the agent's local copy must leave the store's object
	// completely alone — on one machine those are two files on the same
	// disk, and confusing them would delete the only durable copy.
	if got, ok := b.remote("2026/07/foto.jpg"); !ok || got != "content that will be evicted" {
		t.Fatal("eviction removed the object from the store")
	}
	if _, err := os.Stat(filepath.Join(b.backing, "2026/07/foto.jpg")); err == nil {
		t.Error("eviction did not remove the local copy")
	}

	// And recall brings it back from the store on the same host.
	if err := b.agent.Recall(context.Background(), "2026/07/foto.jpg"); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(b.backing, "2026/07/foto.jpg"))
	if err != nil || string(raw) != "content that will be evicted" {
		t.Errorf("recall returned %q, %v", raw, err)
	}
}

func TestSingleBoxRebuildLeavesTheAgentAlone(t *testing.T) {
	b := newSinglebox(t)
	b.write("2026/07/foto.jpg", "content")
	b.write("2026/07/altra.jpg", "more content")
	if err := b.agent.Start(b.stop); err != nil {
		t.Fatal(err)
	}
	b.settle()

	before, err := b.agent.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if before.Objects != 2 {
		t.Fatalf("agent has %d objects before the rebuild", before.Objects)
	}

	// --fsck --rebuild discards the store's index and replays its
	// journal. On one machine that runs against the same file the agent
	// is using, so it must touch only what it owns.
	rep, err := b.store.Fsck(true)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.Rebuilt || rep.Objects != 2 {
		t.Fatalf("rebuild = %+v", rep)
	}
	if !rep.Healthy() {
		t.Errorf("rebuild reported problems: %+v", rep)
	}

	after, err := b.agent.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if after.Objects != before.Objects || after.LocalBytes != before.LocalBytes {
		t.Fatalf("the store's rebuild damaged the agent's index: %+v became %+v", before, after)
	}
	// The agent must still be able to answer for its files.
	if _, ok, _ := b.idx.Get(b.agent.Namespace(), "2026/07/foto.jpg"); !ok {
		t.Error("the agent's entry was dropped by the store's rebuild")
	}
}

func TestSingleBoxFsckIgnoresAgentNamespaces(t *testing.T) {
	b := newSinglebox(t)
	b.write("2026/07/foto.jpg", "content")
	if err := b.agent.Start(b.stop); err != nil {
		t.Fatal(err)
	}
	b.settle()

	// The agent's entries carry no blob hash — they describe local
	// files, not blobs. If fsck walked them it would report every one
	// as a missing blob and call a healthy store broken.
	rep, err := b.store.Fsck(false)
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.MissingBlobs) != 0 {
		t.Errorf("fsck reported the agent's entries as missing blobs: %v", rep.MissingBlobs)
	}
	if rep.Objects != 1 {
		t.Errorf("fsck counted %d objects, want only the store's 1", rep.Objects)
	}
}
