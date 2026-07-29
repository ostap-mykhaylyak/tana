package store

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/ostap-mykhaylyak/tana/internal/index"
)

// Multipart uploads.
//
// WordPress never needs them — media files are small and go up in one
// request — but `aws s3 cp` and `mc` switch to multipart above a size
// threshold, and a store that refuses is a store nobody can administer
// with the tools they already have.
//
// Each part is stored as an ordinary blob, so parts dedupe against
// each other and against existing objects. Completion concatenates
// them into one blob and releases the parts. An upload in progress is
// a small JSON record in the index; the parts it references are
// reference-counted like anything else, which is what keeps the
// collector from eating a slow upload's parts out from under it.

// uploadPrefix namespaces in-progress uploads in the index metadata.
const uploadPrefix = "mpu/"

// maxParts is the S3 limit, and a sane bound on the state record.
const maxParts = 10000

// Upload is a multipart upload in progress.
type Upload struct {
	Bucket    string              `json:"bucket"`
	Key       string              `json:"key"`
	ID        string              `json:"id"`
	Initiated time.Time           `json:"initiated"`
	Parts     map[int]*UploadPart `json:"parts"`
}

// UploadPart is one uploaded part.
type UploadPart struct {
	Hash     string    `json:"hash"`
	ETag     string    `json:"etag"`
	Size     int64     `json:"size"`
	Uploaded time.Time `json:"uploaded"`
}

// ErrNoSuchUpload is returned for an unknown upload id.
type ErrNoSuchUpload struct{ Bucket, ID string }

func (e ErrNoSuchUpload) Error() string { return "no such upload: " + e.Bucket + "/" + e.ID }

// ErrInvalidPart reports that a completion referenced a part that was
// never uploaded, or whose tag does not match.
type ErrInvalidPart struct{ Number int }

func (e ErrInvalidPart) Error() string { return fmt.Sprintf("invalid part %d", e.Number) }

// uploadKey is where an upload's state lives in the index metadata.
func uploadKey(bucket, id string) string { return uploadPrefix + bucket + "/" + id }

// CreateUpload starts a multipart upload and returns its id.
func (s *Store) CreateUpload(bucket, key string) (*Upload, error) {
	if _, ok := s.Bucket(bucket); !ok {
		return nil, ErrNoSuchBucket{Name: bucket}
	}
	if key == "" {
		return nil, fmt.Errorf("store: empty key")
	}
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return nil, fmt.Errorf("store: upload id: %w", err)
	}
	u := &Upload{
		Bucket:    bucket,
		Key:       key,
		ID:        hex.EncodeToString(raw[:]),
		Initiated: time.Now().UTC(),
		Parts:     map[int]*UploadPart{},
	}
	if err := s.saveUpload(u); err != nil {
		return nil, err
	}
	s.xferLog.Info("multipart created", "bucket", bucket, "key", key, "upload", u.ID)
	return u, nil
}

// UploadPart stores one part.
//
// The part is journalled as nothing: it is not an object, and a crash
// mid-upload should leave no trace beyond an orphan blob the collector
// removes. Only completion produces a durable object.
func (s *Store) UploadPart(bucket, id string, number int, r io.Reader) (*UploadPart, error) {
	if number < 1 || number > maxParts {
		return nil, fmt.Errorf("store: part number %d out of range", number)
	}
	u, err := s.loadUpload(bucket, id)
	if err != nil {
		return nil, err
	}

	info, err := s.blobs.Put(r)
	if err != nil {
		return nil, err
	}
	// Hold a reference so the collector does not take the part while
	// the upload is still open.
	if err := s.idx.Update(func(tx *index.Tx) error {
		_, err := tx.Ref(info.Hash, +1, time.Now().UTC())
		return err
	}); err != nil {
		return nil, err
	}

	// Replacing a part releases the one it supersedes.
	if old, ok := u.Parts[number]; ok {
		if err := s.releaseBlob(old.Hash); err != nil {
			return nil, err
		}
	}
	part := &UploadPart{Hash: info.Hash, ETag: info.MD5, Size: info.Size, Uploaded: time.Now().UTC()}
	u.Parts[number] = part
	if err := s.saveUpload(u); err != nil {
		return nil, err
	}
	return part, nil
}

// CompleteUpload assembles the listed parts into one object.
//
// The client says which parts it uploaded and in what order; anything
// it uploaded and did not list is discarded. The resulting entity tag
// follows S3's convention — the md5 of the concatenated part md5s,
// suffixed with the part count — which is why it cannot be recomputed
// from the finished object and has to be recorded.
func (s *Store) CompleteUpload(bucket, id string, parts []CompletedPart) (index.Entry, error) {
	u, err := s.loadUpload(bucket, id)
	if err != nil {
		return index.Entry{}, err
	}
	if len(parts) == 0 {
		return index.Entry{}, fmt.Errorf("store: completion listed no parts")
	}
	sort.Slice(parts, func(a, b int) bool { return parts[a].Number < parts[b].Number })

	readers := make([]io.Reader, 0, len(parts))
	closers := make([]io.Closer, 0, len(parts))
	defer func() {
		for _, c := range closers {
			c.Close()
		}
	}()

	sum := md5.New()
	for _, p := range parts {
		got, ok := u.Parts[p.Number]
		if !ok || !etagEqual(got.ETag, p.ETag) {
			return index.Entry{}, ErrInvalidPart{Number: p.Number}
		}
		f, err := s.blobs.Open(got.Hash)
		if err != nil {
			return index.Entry{}, err
		}
		readers = append(readers, f)
		closers = append(closers, f)

		raw, err := hex.DecodeString(got.ETag)
		if err != nil {
			return index.Entry{}, fmt.Errorf("store: unreadable part etag: %w", err)
		}
		sum.Write(raw)
	}
	etag := fmt.Sprintf("%s-%d", hex.EncodeToString(sum.Sum(nil)), len(parts))

	entry, err := s.PutWith(bucket, u.Key, io.MultiReader(readers...), PutOptions{ETag: etag})
	if err != nil {
		return index.Entry{}, err
	}
	if err := s.discardUpload(u); err != nil {
		return index.Entry{}, err
	}
	s.xferLog.Info("multipart completed",
		"bucket", bucket, "key", u.Key, "upload", id, "parts", len(parts), "size", entry.Size)
	return entry, nil
}

// CompletedPart is one entry of a completion request.
type CompletedPart struct {
	Number int
	ETag   string
}

// AbortUpload discards an upload and releases its parts.
func (s *Store) AbortUpload(bucket, id string) error {
	u, err := s.loadUpload(bucket, id)
	if err != nil {
		return err
	}
	if err := s.discardUpload(u); err != nil {
		return err
	}
	s.xferLog.Info("multipart aborted", "bucket", bucket, "key", u.Key, "upload", id)
	return nil
}

// ListParts returns an upload's parts in order.
func (s *Store) ListParts(bucket, id string) (*Upload, []int, error) {
	u, err := s.loadUpload(bucket, id)
	if err != nil {
		return nil, nil, err
	}
	numbers := make([]int, 0, len(u.Parts))
	for n := range u.Parts {
		numbers = append(numbers, n)
	}
	sort.Ints(numbers)
	return u, numbers, nil
}

// ListUploads returns the uploads in progress for a bucket.
func (s *Store) ListUploads(bucket string) ([]*Upload, error) {
	if _, ok := s.Bucket(bucket); !ok {
		return nil, ErrNoSuchBucket{Name: bucket}
	}
	var out []*Upload
	err := s.idx.View(func(tx *index.Tx) error {
		return tx.WalkMeta(uploadPrefix+bucket+"/", func(_ string, raw []byte) error {
			var u Upload
			if err := json.Unmarshal(raw, &u); err != nil {
				return err
			}
			out = append(out, &u)
			return nil
		})
	})
	sort.Slice(out, func(a, b int) bool { return out[a].Initiated.Before(out[b].Initiated) })
	return out, err
}

// discardUpload removes the state record and releases every part.
func (s *Store) discardUpload(u *Upload) error {
	for _, p := range u.Parts {
		if err := s.releaseBlob(p.Hash); err != nil {
			return err
		}
	}
	return s.idx.Update(func(tx *index.Tx) error {
		return tx.DelMeta(uploadKey(u.Bucket, u.ID))
	})
}

// releaseBlob drops one reference held by an upload part. The blob
// itself is left to the collector, exactly as a deleted object is.
func (s *Store) releaseBlob(hash string) error {
	return s.idx.Update(func(tx *index.Tx) error {
		_, err := tx.Ref(hash, -1, time.Now().UTC())
		return err
	})
}

func (s *Store) saveUpload(u *Upload) error {
	raw, err := json.Marshal(u)
	if err != nil {
		return err
	}
	return s.idx.Update(func(tx *index.Tx) error {
		return tx.SetMeta(uploadKey(u.Bucket, u.ID), raw)
	})
}

func (s *Store) loadUpload(bucket, id string) (*Upload, error) {
	if _, ok := s.Bucket(bucket); !ok {
		return nil, ErrNoSuchBucket{Name: bucket}
	}
	raw, err := s.idx.Meta(uploadKey(bucket, id))
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return nil, ErrNoSuchUpload{Bucket: bucket, ID: id}
	}
	var u Upload
	if err := json.Unmarshal(raw, &u); err != nil {
		return nil, fmt.Errorf("store: corrupt upload record %s/%s: %w", bucket, id, err)
	}
	if u.Parts == nil {
		u.Parts = map[int]*UploadPart{}
	}
	return &u, nil
}

// etagEqual compares entity tags ignoring the quotes clients wrap them
// in, which some send and some do not.
func etagEqual(a, b string) bool {
	return trimETag(a) == trimETag(b)
}

func trimETag(s string) string {
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return s[1 : len(s)-1]
	}
	return s
}
