package agent

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ostap-mykhaylyak/tana/internal/config"
	"github.com/ostap-mykhaylyak/tana/internal/index"
)

// local reports whether an object's bytes are in the backing store.
func (h *harness) local(key string) bool {
	h.t.Helper()
	_, err := os.Stat(filepath.Join(h.backing, filepath.FromSlash(key)))
	return err == nil
}

func TestEvictThenRecall(t *testing.T) {
	h := newHarness(t)
	h.write("2026/07/foto.jpg", "the original bytes")
	if err := h.Start(h.stop); err != nil {
		t.Fatal(err)
	}
	h.settle()

	before, _, err := h.idx.Get(h.Name(), "2026/07/foto.jpg")
	if err != nil {
		t.Fatal(err)
	}

	if err := h.Evict("2026/07/foto.jpg"); err != nil {
		t.Fatal(err)
	}
	if h.local("2026/07/foto.jpg") {
		t.Fatal("eviction left the file on disk")
	}

	// The whole point: the metadata is still local and still true, so
	// every stat WordPress makes is answered without a network call.
	after, ok, err := h.idx.Get(h.Name(), "2026/07/foto.jpg")
	if err != nil || !ok {
		t.Fatal(err)
	}
	if after.State != index.Evicted {
		t.Errorf("state = %s, want evicted", after.State)
	}
	if after.Size != before.Size || !after.ModTime.Equal(before.ModTime) {
		t.Errorf("eviction changed the metadata: %+v vs %+v", after, before)
	}

	if err := h.Recall(context.Background(), "2026/07/foto.jpg"); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(h.backing, "2026/07/foto.jpg"))
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != "the original bytes" {
		t.Errorf("recalled content = %q", raw)
	}

	// The modification time must survive the round trip: plugins
	// compare it against the database, and a file that comes back
	// looking newer sets off work that has no reason to happen.
	fi, err := os.Stat(filepath.Join(h.backing, "2026/07/foto.jpg"))
	if err != nil {
		t.Fatal(err)
	}
	if !fi.ModTime().Truncate(time.Second).Equal(before.ModTime.Truncate(time.Second)) {
		t.Errorf("recalled mtime = %v, want %v", fi.ModTime(), before.ModTime)
	}

	if s, _ := h.state("2026/07/foto.jpg"); s != index.Synced {
		t.Errorf("state after recall = %s, want synced", s)
	}
}

func TestRecallOfUnknownKey(t *testing.T) {
	h := newHarness(t)
	err := h.Recall(context.Background(), "never-existed.jpg")
	if !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("err = %v, want ErrNotExist", err)
	}
}

func TestRecallOfPresentObjectIsANoOp(t *testing.T) {
	h := newHarness(t)
	h.write("a.jpg", "here already")
	if err := h.Start(h.stop); err != nil {
		t.Fatal(err)
	}
	h.settle()

	if err := h.Recall(context.Background(), "a.jpg"); err != nil {
		t.Fatal(err)
	}
	if !h.local("a.jpg") {
		t.Error("recall removed a file that was already local")
	}
}

func TestConcurrentRecallsCollapse(t *testing.T) {
	h := newHarness(t)
	h.write("hot.jpg", "twenty requests want this")
	if err := h.Start(h.stop); err != nil {
		t.Fatal(err)
	}
	h.settle()
	if err := h.Evict("hot.jpg"); err != nil {
		t.Fatal(err)
	}

	// A cold page with twenty images must not open twenty connections
	// for the same file.
	var wg sync.WaitGroup
	errs := make([]error, 20)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = h.Recall(context.Background(), "hot.jpg")
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("concurrent recall %d failed: %v", i, err)
		}
	}
	if !h.local("hot.jpg") {
		t.Fatal("the file was not recalled")
	}
}

func TestEvictRefusesDirtyObjects(t *testing.T) {
	h := newHarness(t)
	h.write("unsent.jpg", "not on the store yet")
	if _, err := h.Scan(); err != nil {
		t.Fatal(err)
	}

	// Evicting here would delete the only copy that exists.
	if err := h.Evict("unsent.jpg"); err == nil {
		t.Fatal("evicted an object that had never been uploaded")
	}
	if !h.local("unsent.jpg") {
		t.Fatal("the file was removed anyway")
	}
}

func TestEvictIsIdempotent(t *testing.T) {
	h := newHarness(t)
	h.write("a.jpg", "content")
	if err := h.Start(h.stop); err != nil {
		t.Fatal(err)
	}
	h.settle()

	if err := h.Evict("a.jpg"); err != nil {
		t.Fatal(err)
	}
	if err := h.Evict("a.jpg"); err != nil {
		t.Errorf("second Evict: %v", err)
	}
}

// fill writes n files of the given size and waits for them to upload.
func (h *harness) fill(n, size int, prefix string) {
	h.t.Helper()
	content := make([]byte, size)
	for i := range content {
		content[i] = byte('a' + i%26)
	}
	for i := 0; i < n; i++ {
		h.write(prefix+string(rune('a'+i))+".jpg", string(content))
	}
}

func TestEvictToFitRespectsTheCeiling(t *testing.T) {
	h := newHarness(t)
	h.site.Cache = config.Cache{MaxSize: 300, KeepBelow: 0}

	h.fill(5, 100, "big-") // 500 bytes, ceiling is 300
	if err := h.Start(h.stop); err != nil {
		t.Fatal(err)
	}
	h.settle()

	st, err := h.EvictToFit(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if st.Evicted == 0 {
		t.Fatal("nothing was evicted despite being over the ceiling")
	}
	stats, err := h.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.LocalBytes > 300 {
		t.Errorf("local bytes = %d, still over the 300 byte ceiling", stats.LocalBytes)
	}
	// Nothing was lost: the objects are still known, just not local.
	if stats.Objects != 5 {
		t.Errorf("objects = %d, want 5 — eviction must not lose entries", stats.Objects)
	}
}

func TestEvictToFitKeepsSmallFiles(t *testing.T) {
	h := newHarness(t)
	// Thumbnails are what plugins stat and read constantly; evicting
	// them frees nothing and slows every page.
	h.site.Cache = config.Cache{MaxSize: 10, KeepBelow: 200}

	h.fill(5, 100, "thumb-")
	if err := h.Start(h.stop); err != nil {
		t.Fatal(err)
	}
	h.settle()

	st, err := h.EvictToFit(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if st.Evicted != 0 {
		t.Errorf("evicted %d file(s) below the size threshold", st.Evicted)
	}
}

func TestEvictToFitHonoursPinsAndPatterns(t *testing.T) {
	h := newHarness(t)
	h.site.Cache = config.Cache{
		MaxSize:    1,
		NeverEvict: []string{"woocommerce_uploads/**"},
	}
	h.write("woocommerce_uploads/2026/07/manual.pdf", "a protected download")
	h.write("2026/07/pinned.jpg", "explicitly pinned")
	h.write("2026/07/ordinary.jpg", "fair game")
	if err := h.Start(h.stop); err != nil {
		t.Fatal(err)
	}
	h.settle()

	if err := h.Pin("2026/07/pinned.jpg", true); err != nil {
		t.Fatal(err)
	}
	if _, err := h.EvictToFit(time.Now()); err != nil {
		t.Fatal(err)
	}

	if !h.local("woocommerce_uploads/2026/07/manual.pdf") {
		t.Error("a never_evict pattern did not protect its key")
	}
	if !h.local("2026/07/pinned.jpg") {
		t.Error("a pinned object was evicted")
	}
	if h.local("2026/07/ordinary.jpg") {
		t.Error("the unprotected object should have been evicted")
	}
}

func TestEvictToFitTakesTheColdestFirst(t *testing.T) {
	h := newHarness(t)
	h.site.Cache = config.Cache{MaxSize: 150}
	h.write("cold.jpg", string(make([]byte, 100)))
	h.write("warm.jpg", string(make([]byte, 100)))
	if err := h.Start(h.stop); err != nil {
		t.Fatal(err)
	}
	h.settle()

	// warm.jpg was read recently; cold.jpg last year.
	h.idx.Touch(h.Name(), "cold.jpg", time.Now().Add(-365*24*time.Hour))
	h.idx.Touch(h.Name(), "warm.jpg", time.Now())

	if _, err := h.EvictToFit(time.Now()); err != nil {
		t.Fatal(err)
	}
	if h.local("cold.jpg") {
		t.Error("the cold object survived while the warm one was considered")
	}
	if !h.local("warm.jpg") {
		t.Error("the recently read object was evicted first")
	}
}

func TestEvictToFitDoesNothingWithoutACeiling(t *testing.T) {
	h := newHarness(t)
	h.site.Cache = config.Cache{} // mirror everything
	h.fill(3, 100, "x-")
	if err := h.Start(h.stop); err != nil {
		t.Fatal(err)
	}
	h.settle()

	st, err := h.EvictToFit(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if st.Evicted != 0 || st.Candidates != 0 {
		t.Errorf("evicted %d with no ceiling configured", st.Evicted)
	}
}

func TestMatchGlob(t *testing.T) {
	cases := []struct {
		pattern, key string
		want         bool
	}{
		{"woocommerce_uploads/**", "woocommerce_uploads/2026/07/a.pdf", true},
		{"woocommerce_uploads/**", "woocommerce_uploads", true},
		{"woocommerce_uploads/**", "other/2026/a.jpg", false},
		{"**", "anything/at/all", true},
		{"*.pdf", "manual.pdf", true},
		{"*.pdf", "2026/manual.pdf", false}, // a single star does not cross separators
		{"2026/*/a.jpg", "2026/07/a.jpg", true},
		{"2026/*/a.jpg", "2026/07/08/a.jpg", false},
	}
	for _, c := range cases {
		if got := matchGlob(c.pattern, c.key); got != c.want {
			t.Errorf("matchGlob(%q, %q) = %v, want %v", c.pattern, c.key, got, c.want)
		}
	}
}
