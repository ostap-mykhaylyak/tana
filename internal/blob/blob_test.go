package blob

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func open(t *testing.T) *Store {
	t.Helper()
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func put(t *testing.T, s *Store, content string) Info {
	t.Helper()
	info, err := s.Put(strings.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	return info
}

func TestPutIsContentAddressed(t *testing.T) {
	s := open(t)
	const content = "the quick brown fox"
	info := put(t, s, content)

	want := sha256.Sum256([]byte(content))
	if info.Hash != hex.EncodeToString(want[:]) {
		t.Fatalf("hash = %s, want the sha256 of the content", info.Hash)
	}
	if info.Size != int64(len(content)) {
		t.Errorf("size = %d, want %d", info.Size, len(content))
	}
	if info.Deduped {
		t.Error("a first write must not report dedup")
	}

	f, err := s.Open(info.Hash)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	got := make([]byte, len(content))
	if _, err := f.Read(got); err != nil {
		t.Fatal(err)
	}
	if string(got) != content {
		t.Errorf("read back %q", got)
	}
}

func TestPutDedupes(t *testing.T) {
	s := open(t)
	first := put(t, s, "same bytes")
	second := put(t, s, "same bytes")

	if second.Hash != first.Hash {
		t.Fatal("identical content produced different hashes")
	}
	if !second.Deduped {
		t.Error("the second write of identical content must report dedup")
	}
	count, _, err := s.Usage()
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("blobs on disk = %d, want 1", count)
	}
}

func TestPutLeavesNoTempFiles(t *testing.T) {
	s := open(t)
	put(t, s, "a")
	put(t, s, "a") // the deduped path must clean up after itself too
	entries, err := os.ReadDir(s.tmp())
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Errorf("tmp/ holds %d leftover file(s)", len(entries))
	}
}

func TestOpenClearsStaleTemps(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	// A crash mid-Put leaves a partial file nobody knows the name of.
	stale := filepath.Join(s.tmp(), "leftover")
	if err := os.WriteFile(stale, []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(stale); !errors.Is(err, os.ErrNotExist) {
		t.Error("a stale temp file survived Open")
	}
}

func TestStatAndRemove(t *testing.T) {
	s := open(t)
	info := put(t, s, "hello")

	size, ok, err := s.Stat(info.Hash)
	if err != nil || !ok || size != 5 {
		t.Fatalf("Stat = %d, %v, %v", size, ok, err)
	}
	if err := s.Remove(info.Hash); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := s.Stat(info.Hash); ok {
		t.Error("blob survived Remove")
	}
	// The collector must be safe to run twice over the same list.
	if err := s.Remove(info.Hash); err != nil {
		t.Errorf("second Remove: %v", err)
	}
}

func TestOpenMissingIsNotFound(t *testing.T) {
	s := open(t)
	_, err := s.Open(strings.Repeat("a", 64))
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}

func TestVerifyDetectsCorruption(t *testing.T) {
	s := open(t)
	info := put(t, s, "trust but verify")

	ok, err := s.Verify(info.Hash)
	if err != nil || !ok {
		t.Fatalf("Verify on a healthy blob = %v, %v", ok, err)
	}

	// Rot the bytes underneath, leaving the name — exactly what a
	// scrub exists to catch.
	if err := os.WriteFile(s.Path(info.Hash), []byte("tampered"), 0o600); err != nil {
		t.Fatal(err)
	}
	ok, err = s.Verify(info.Hash)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Error("Verify accepted a blob whose content no longer matches its name")
	}
}

func TestValidHash(t *testing.T) {
	good := strings.Repeat("0123456789abcdef", 4) // 64 hex chars
	if err := ValidHash(good); err != nil {
		t.Errorf("rejected a valid hash: %v", err)
	}
	for _, bad := range []string{
		"",
		"abc",
		strings.Repeat("a", 63),
		strings.Repeat("a", 65),
		strings.Repeat("A", 64), // uppercase
		strings.Repeat("g", 64), // not hex
		"../../etc/passwd",      // the reason this function exists
		strings.Repeat("a", 32) + "/" + strings.Repeat("b", 31),
	} {
		if err := ValidHash(bad); err == nil {
			t.Errorf("accepted invalid hash %q", bad)
		}
	}
}

func TestPathRefusesMalformedHash(t *testing.T) {
	s := open(t)
	// Path is the only place a hash becomes a filesystem path, so it
	// must not be possible to escape the store through it.
	if p := s.Path("../../etc/passwd"); p != "" {
		t.Errorf("Path built %q from a malformed hash", p)
	}
	if p := s.Path("short"); p != "" {
		t.Errorf("Path built %q from a short hash", p)
	}
}

func TestPathFanout(t *testing.T) {
	s := open(t)
	hash := strings.Repeat("ab", 32)
	want := filepath.Join(s.Root(), blobsDir, "ab", "ab", hash)
	if got := s.Path(hash); got != want {
		t.Errorf("Path = %s, want %s", got, want)
	}
}

func TestWalkReportsForeignFiles(t *testing.T) {
	s := open(t)
	info := put(t, s, "legitimate")

	// Something that is not ours, in a directory that is.
	foreign := filepath.Join(s.blobs(), "README")
	if err := os.WriteFile(foreign, []byte("hi"), 0o600); err != nil {
		t.Fatal(err)
	}

	var blobs, foreigns int
	err := s.Walk(func(hash string, size int64, mod time.Time, bad bool) error {
		if bad {
			foreigns++
			return nil
		}
		blobs++
		if hash != info.Hash {
			t.Errorf("unexpected blob %s", hash)
		}
		if mod.IsZero() {
			t.Error("Walk reported a zero modification time")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if blobs != 1 || foreigns != 1 {
		t.Errorf("blobs = %d, foreign = %d, want 1 and 1", blobs, foreigns)
	}
}

func TestUsage(t *testing.T) {
	s := open(t)
	put(t, s, "aaaa")
	put(t, s, "bb")
	put(t, s, "aaaa") // deduped, must not be counted twice

	count, bytes, err := s.Usage()
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 || bytes != 6 {
		t.Errorf("Usage = %d blobs / %d bytes, want 2 / 6", count, bytes)
	}
}

func TestPutEmptyContent(t *testing.T) {
	s := open(t)
	info, err := s.Put(bytes.NewReader(nil))
	if err != nil {
		t.Fatal(err)
	}
	if info.Size != 0 {
		t.Errorf("size = %d", info.Size)
	}
	// An empty object is a legitimate S3 object and must be readable.
	if _, ok, _ := s.Stat(info.Hash); !ok {
		t.Error("the empty blob was not stored")
	}
}

func TestHashOf(t *testing.T) {
	s := open(t)
	info := put(t, s, "consistency")
	if got := HashOf([]byte("consistency")); got != info.Hash {
		t.Errorf("HashOf = %s, Put = %s", got, info.Hash)
	}
}
