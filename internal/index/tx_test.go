package index

import (
	"testing"
	"time"
)

func TestRefCountsUpAndDown(t *testing.T) {
	db := open(t)
	const hash = "abc123"

	var count int64
	err := db.Update(func(tx *Tx) error {
		var err error
		count, err = tx.Ref(hash, +1, time.Now())
		return err
	})
	if err != nil || count != 1 {
		t.Fatalf("Ref +1 = %d, %v", count, err)
	}
	if err := db.Update(func(tx *Tx) error {
		count, err = tx.Ref(hash, +1, time.Now())
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("Ref +1 twice = %d, want 2", count)
	}
}

func TestUnrefStampsTheGraceClock(t *testing.T) {
	db := open(t)
	const hash = "abc123"
	at := time.Now()

	if err := db.Update(func(tx *Tx) error {
		if _, err := tx.Ref(hash, +1, at); err != nil {
			return err
		}
		_, err := tx.Ref(hash, -1, at)
		return err
	}); err != nil {
		t.Fatal(err)
	}

	var r Ref
	if err := db.View(func(tx *Tx) error {
		var err error
		r, _, err = tx.RefOf(hash)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if r.Count != 0 {
		t.Fatalf("count = %d, want 0", r.Count)
	}
	if r.UnrefAt.IsZero() {
		t.Fatal("reaching zero must start the grace clock")
	}

	// A referenced blob is not eligible whatever the clock says.
	if r.Collectable(at.Add(time.Hour), 2*time.Hour) {
		t.Error("collectable before the grace period elapsed")
	}
	if !r.Collectable(at.Add(3*time.Hour), 2*time.Hour) {
		t.Error("not collectable after the grace period elapsed")
	}
}

func TestReReferencingClearsTheGraceClock(t *testing.T) {
	db := open(t)
	const hash = "abc123"
	at := time.Now()

	// Down to zero, then back up: the blob must stop being a candidate,
	// or a re-uploaded file gets deleted out from under its new key.
	if err := db.Update(func(tx *Tx) error {
		if _, err := tx.Ref(hash, +1, at); err != nil {
			return err
		}
		if _, err := tx.Ref(hash, -1, at); err != nil {
			return err
		}
		_, err := tx.Ref(hash, +1, at.Add(time.Minute))
		return err
	}); err != nil {
		t.Fatal(err)
	}

	var r Ref
	if err := db.View(func(tx *Tx) error {
		var err error
		r, _, err = tx.RefOf(hash)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if r.Count != 1 || !r.UnrefAt.IsZero() {
		t.Fatalf("ref = %+v, want count 1 and no grace clock", r)
	}
	if r.Collectable(at.Add(100*time.Hour), time.Hour) {
		t.Error("a re-referenced blob is still collectable")
	}
}

func TestRefOfUnknownHash(t *testing.T) {
	db := open(t)
	if err := db.View(func(tx *Tx) error {
		_, ok, err := tx.RefOf("nothing")
		if ok {
			t.Error("RefOf reported a hash that was never referenced")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func TestDropRef(t *testing.T) {
	db := open(t)
	const hash = "abc123"
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.Ref(hash, +1, time.Now())
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error { return tx.DropRef(hash) }); err != nil {
		t.Fatal(err)
	}
	if err := db.View(func(tx *Tx) error {
		_, ok, err := tx.RefOf(hash)
		if ok {
			t.Error("the reference record survived DropRef")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func TestWalkRefs(t *testing.T) {
	db := open(t)
	if err := db.Update(func(tx *Tx) error {
		for _, h := range []string{"a", "b", "c"} {
			if _, err := tx.Ref(h, +1, time.Now()); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	seen := 0
	if err := db.WalkRefs(func(hash string, r Ref) error {
		seen++
		if r.Count != 1 {
			t.Errorf("%s count = %d", hash, r.Count)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if seen != 3 {
		t.Errorf("walked %d refs, want 3", seen)
	}
}

func TestRefRejectsEmptyHash(t *testing.T) {
	db := open(t)
	err := db.Update(func(tx *Tx) error {
		_, err := tx.Ref("", +1, time.Now())
		return err
	})
	if err == nil {
		t.Error("accepted an empty hash")
	}
}

func TestMetaRoundTrip(t *testing.T) {
	db := open(t)
	if got, err := db.Meta("absent"); err != nil || got != nil {
		t.Fatalf("Meta on a missing key = %q, %v", got, err)
	}
	if err := db.SetMeta("position", []byte("42")); err != nil {
		t.Fatal(err)
	}
	got, err := db.Meta("position")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "42" {
		t.Errorf("Meta = %q, want 42", got)
	}
}

func TestTxComposesObjectAndRefAtomically(t *testing.T) {
	db := open(t)
	// The point of the Tx API: a failure part-way must leave neither
	// the object nor its reference behind.
	sentinel := errAfterWrite
	err := db.Update(func(tx *Tx) error {
		if err := tx.Put("shop", entry("a.jpg", 10, Synced)); err != nil {
			return err
		}
		if _, err := tx.Ref("hash-a", +1, time.Now()); err != nil {
			return err
		}
		return sentinel
	})
	if err != sentinel {
		t.Fatalf("err = %v, want the sentinel", err)
	}
	if _, ok, _ := db.Get("shop", "a.jpg"); ok {
		t.Error("the object survived a rolled-back transaction")
	}
	if err := db.View(func(tx *Tx) error {
		_, ok, err := tx.RefOf("hash-a")
		if ok {
			t.Error("the reference survived a rolled-back transaction")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

// errAfterWrite forces a rollback in TestTxComposesObjectAndRefAtomically.
var errAfterWrite = errSentinel{}

type errSentinel struct{}

func (errSentinel) Error() string { return "rollback" }
