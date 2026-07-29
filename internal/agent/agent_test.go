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

// The agent is tested against a real store behind a real S3 server, so
// what is verified is that the two halves of tana interoperate over
// the wire — signatures, encoding, error codes and all. A mocked
// backend would test only that the agent calls the methods the mock
// declares.

const (
	testBucket = "shop-uploads"
	testAK     = "TANATESTACCESSKEY000"
	testSK     = "testsecretkey0123456789abcdefghij"
	testRegion = "tana"
)

type harness struct {
	*Agent
	store   *store.Store
	backing string
	stop    chan struct{}
	t       *testing.T
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	dir := t.TempDir()
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))

	// The store side.
	storeIdx, err := index.Open(filepath.Join(dir, "store-index.db"))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.New(config.Store{
		Data:    filepath.Join(dir, "data"),
		Region:  testRegion,
		Buckets: []config.Bucket{{Name: testBucket, AccessKey: testAK, SecretKey: testSK}},
		GC:      config.GC{Interval: config.Duration(time.Hour), Grace: config.Duration(72 * time.Hour)},
	}, storeIdx, discard, discard)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(s3.New(st, testRegion, discard, discard))

	// The agent side.
	agentIdx, err := index.Open(filepath.Join(dir, "agent-index.db"))
	if err != nil {
		t.Fatal(err)
	}
	backing := filepath.Join(dir, "backing")
	site := config.Site{
		Name:    "shop.example.com",
		Uploads: filepath.Join(dir, "uploads"),
		Backing: backing,
		Backend: config.Backend{
			Endpoint: srv.URL, Bucket: testBucket, Region: testRegion,
			AccessKey: testAK, SecretKey: testSK, Ack: config.AckRemote,
		},
	}
	a, err := New(site, agentIdx, discard, discard)
	if err != nil {
		t.Fatal(err)
	}

	stop := make(chan struct{})
	t.Cleanup(func() {
		close(stop)
		a.Wait()
		srv.Close()
		st.Close()
		storeIdx.Close()
		agentIdx.Close()
	})
	return &harness{Agent: a, store: st, backing: backing, stop: stop, t: t}
}

// write puts a file in the backing store, the way WordPress would.
func (h *harness) write(rel, content string) {
	h.t.Helper()
	p := filepath.Join(h.backing, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
		h.t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o640); err != nil {
		h.t.Fatal(err)
	}
}

// settle drains the upload queue.
func (h *harness) settle() {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := h.Drain(ctx); err != nil {
		h.t.Fatalf("queue did not drain: %v", err)
	}
}

// remote reads an object back from the store.
func (h *harness) remote(key string) (string, bool) {
	h.t.Helper()
	_, rc, err := h.store.Get(testBucket, key)
	if err != nil {
		return "", false
	}
	defer rc.Close()
	raw, err := io.ReadAll(rc)
	if err != nil {
		h.t.Fatal(err)
	}
	return string(raw), true
}

// state returns an object's state in the agent's index.
func (h *harness) state(key string) (index.State, bool) {
	h.t.Helper()
	e, ok, err := h.idx.Get(h.Name(), key)
	if err != nil {
		h.t.Fatal(err)
	}
	return e.State, ok
}

func TestScanUploadsExistingFiles(t *testing.T) {
	h := newHarness(t)
	h.write("2026/07/foto.jpg", "a photo")
	h.write("2026/07/foto-150x150.jpg", "a thumbnail")

	st, err := h.Scan()
	if err != nil {
		t.Fatal(err)
	}
	if st.Files != 2 || st.New != 2 {
		t.Fatalf("scan = %+v, want 2 files, 2 new", st)
	}
	// Before the workers run, both are dirty: bytes here and nowhere
	// else.
	if s, _ := h.state("2026/07/foto.jpg"); s != index.Dirty {
		t.Errorf("state before upload = %s, want dirty", s)
	}

	if err := h.Start(h.stop); err != nil {
		t.Fatal(err)
	}
	h.settle()

	for _, k := range []string{"2026/07/foto.jpg", "2026/07/foto-150x150.jpg"} {
		if _, ok := h.remote(k); !ok {
			t.Errorf("%s never reached the store", k)
		}
		if s, _ := h.state(k); s != index.Synced {
			t.Errorf("%s state = %s, want synced", k, s)
		}
	}
	got, _ := h.remote("2026/07/foto.jpg")
	if got != "a photo" {
		t.Errorf("stored content = %q", got)
	}
}

func TestNewFilesAreUploadedWhileRunning(t *testing.T) {
	h := newHarness(t)
	if err := h.Start(h.stop); err != nil {
		t.Fatal(err)
	}

	// A file that appears after the agent is up, which is the normal
	// case: WordPress writes it during a request.
	h.write("2026/08/nuova.png", "fresh upload")

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := h.remote("2026/08/nuova.png"); ok {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("a file created while running never reached the store")
}

func TestChangedFileIsReuploaded(t *testing.T) {
	h := newHarness(t)
	h.write("a.txt", "version one")
	if err := h.Start(h.stop); err != nil {
		t.Fatal(err)
	}
	h.settle()

	// WordPress does not overwrite uploads, but plugins that optimize
	// images do, and the agent must notice.
	h.write("a.txt", "version two, longer")
	st, err := h.Scan()
	if err != nil {
		t.Fatal(err)
	}
	if st.Changed != 1 {
		t.Fatalf("scan = %+v, want one changed file", st)
	}
	h.settle()

	if got, _ := h.remote("a.txt"); got != "version two, longer" {
		t.Errorf("store still holds %q", got)
	}
}

func TestDeletionIsPropagated(t *testing.T) {
	h := newHarness(t)
	h.write("gone.jpg", "delete me")
	h.write("stays.jpg", "keep me")
	if err := h.Start(h.stop); err != nil {
		t.Fatal(err)
	}
	h.settle()

	if err := os.Remove(filepath.Join(h.backing, "gone.jpg")); err != nil {
		t.Fatal(err)
	}
	st, err := h.Scan()
	if err != nil {
		t.Fatal(err)
	}
	if st.Removed != 1 {
		t.Fatalf("scan = %+v, want one removal", st)
	}

	if _, ok := h.remote("gone.jpg"); ok {
		t.Error("the deleted object is still in the store")
	}
	if _, ok := h.state("gone.jpg"); ok {
		t.Error("the deleted object is still in the index")
	}
	if _, ok := h.remote("stays.jpg"); !ok {
		t.Error("the deletion took an unrelated object with it")
	}
}

func TestEvictedEntriesAreNotTreatedAsDeletions(t *testing.T) {
	h := newHarness(t)
	h.write("cold.jpg", "rarely read")
	if err := h.Start(h.stop); err != nil {
		t.Fatal(err)
	}
	h.settle()

	// Eviction removes the local file on purpose (M5). A scan must not
	// read that as "WordPress deleted it" and wipe the object from the
	// store — which would turn a cache eviction into data loss.
	if _, err := h.idx.SetState(h.Name(), "cold.jpg", index.Evicted); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(h.backing, "cold.jpg")); err != nil {
		t.Fatal(err)
	}

	st, err := h.Scan()
	if err != nil {
		t.Fatal(err)
	}
	if st.Removed != 0 {
		t.Fatalf("scan removed %d evicted object(s)", st.Removed)
	}
	if _, ok := h.remote("cold.jpg"); !ok {
		t.Fatal("an evicted object was deleted from the store")
	}
}

func TestDirtyWorkSurvivesARestart(t *testing.T) {
	h := newHarness(t)
	h.write("pending.jpg", "queued but never sent")

	// Index it as dirty without running the workers: the state a crash
	// mid-upload leaves behind.
	if _, err := h.Scan(); err != nil {
		t.Fatal(err)
	}
	if s, _ := h.state("pending.jpg"); s != index.Dirty {
		t.Fatalf("state = %s, want dirty", s)
	}

	// Starting the agent must find it from the index alone. Nothing in
	// memory survived, and nothing needed to.
	if err := h.Start(h.stop); err != nil {
		t.Fatal(err)
	}
	h.settle()

	if _, ok := h.remote("pending.jpg"); !ok {
		t.Fatal("work left dirty by a previous run was never picked up")
	}
}

func TestKeysUseForwardSlashes(t *testing.T) {
	h := newHarness(t)
	h.write("2026/07/nested/deep.jpg", "nested")
	if err := h.Start(h.stop); err != nil {
		t.Fatal(err)
	}
	h.settle()

	// The key must be an S3 key, not a filesystem path, whatever the
	// separator on the machine that produced it.
	if _, ok := h.remote("2026/07/nested/deep.jpg"); !ok {
		t.Error("nested key did not round-trip as a slash-separated key")
	}
}

func TestUnchangedFilesAreNotReuploaded(t *testing.T) {
	h := newHarness(t)
	h.write("stable.jpg", "unchanged")
	if err := h.Start(h.stop); err != nil {
		t.Fatal(err)
	}
	h.settle()

	st, err := h.Scan()
	if err != nil {
		t.Fatal(err)
	}
	if st.New != 0 || st.Changed != 0 {
		t.Errorf("a second scan found work to do: %+v", st)
	}
}

func TestScanCountsAndStats(t *testing.T) {
	h := newHarness(t)
	h.write("a.jpg", "aaa")
	h.write("b.jpg", "bbbbb")
	if err := h.Start(h.stop); err != nil {
		t.Fatal(err)
	}
	h.settle()

	stats, err := h.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.Objects != 2 || stats.Bytes != 8 {
		t.Errorf("stats = %+v, want 2 objects and 8 bytes", stats)
	}
	if stats.DirtyObjects != 0 {
		t.Errorf("%d object(s) still dirty after the queue drained", stats.DirtyObjects)
	}
}
