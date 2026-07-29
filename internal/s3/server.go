// Package s3 serves the subset of the S3 API that WordPress,
// WooCommerce and the ordinary administration tools actually use.
//
// That subset is small — around a dozen operations — and the ones left
// out are left out on purpose. Versioning, lifecycle rules, object
// lock, bucket policies and server-side encryption negotiation each
// carry a state machine of their own, and none of them has anything to
// do with holding a site's media. They answer NotImplemented, which a
// client can act on, rather than a silent success it cannot.
//
// Addressing is path-style (/bucket/key). Virtual-host style needs a
// wildcard DNS record pointing at the store, which is a lot of moving
// parts for a service that is meant to sit on a private network.
package s3

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/ostap-mykhaylyak/tana/internal/index"
	"github.com/ostap-mykhaylyak/tana/internal/store"
)

const (
	// defaultMaxKeys matches S3's default page size.
	defaultMaxKeys = 1000
	// maxMaxKeys is the ceiling S3 enforces on max-keys.
	maxMaxKeys = 1000
	// maxObjectSize bounds a single-request upload. Multipart exists
	// above it.
	maxObjectSize = 5 << 30
	// maxDeleteBatch is S3's limit for one DeleteObjects request.
	maxDeleteBatch = 1000
	// storageClass is what tana reports; there is only one tier.
	storageClass = "STANDARD"
)

// Server is the S3 HTTP front end.
type Server struct {
	store  *store.Store
	region string
	verify *verifier

	accessLog *slog.Logger
	svcLog    *slog.Logger
}

// New builds a Server over a store.
func New(st *store.Store, region string, accessLog, svcLog *slog.Logger) *Server {
	return &Server{
		store:  st,
		region: region,
		verify: &verifier{
			region: region,
			now:    time.Now,
			lookup: func(accessKey string) (credentials, bool) {
				b, ok := st.BucketByAccessKey(accessKey)
				if !ok {
					return credentials{}, false
				}
				return credentials{AccessKey: b.AccessKey, SecretKey: b.SecretKey, Bucket: b.Name}, true
			},
		},
		accessLog: accessLog,
		svcLog:    svcLog,
	}
}

// ServeHTTP routes and authenticates every request.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	rec := &recorder{ResponseWriter: w, status: http.StatusOK}

	bucket, key := splitPath(r.URL.Path)
	apiErr := s.route(rec, r, bucket, key)
	if apiErr != nil {
		s.writeError(rec, r, apiErr)
	}

	s.accessLog.Info("request",
		"method", r.Method, "bucket", bucket, "key", key,
		"status", rec.status, "bytes", rec.written,
		"remote", r.RemoteAddr, "duration_ms", time.Since(start).Milliseconds())
}

// route dispatches an authenticated request.
func (s *Server) route(w http.ResponseWriter, r *http.Request, bucket, key string) *APIError {
	sig, apiErr := s.verify.verify(r)
	if apiErr != nil {
		return apiErr
	}
	// One key pair grants access to exactly one bucket. Anything else
	// is a request for someone else's media.
	if bucket != "" && bucket != sig.creds.Bucket {
		return errAccessDenied()
	}

	switch {
	case bucket == "":
		if r.Method != http.MethodGet {
			return errNotImplemented("operations on the service root other than listing buckets")
		}
		return s.listBuckets(w, sig)
	case key == "":
		return s.bucketOp(w, r, bucket, sig)
	default:
		return s.objectOp(w, r, bucket, key, sig)
	}
}

// bucketOp handles requests addressed to a bucket.
func (s *Server) bucketOp(w http.ResponseWriter, r *http.Request, bucket string, sig signature) *APIError {
	q := r.URL.Query()
	switch r.Method {
	case http.MethodHead:
		if _, ok := s.store.Bucket(bucket); !ok {
			return errNoSuchBucket()
		}
		w.Header().Set("x-amz-bucket-region", s.region)
		w.WriteHeader(http.StatusOK)
		return nil
	case http.MethodGet:
		if q.Has("uploads") {
			return s.listUploads(w, bucket)
		}
		if unsupportedBucketQuery(q) != "" {
			return errNotImplemented(unsupportedBucketQuery(q))
		}
		return s.listObjects(w, r, bucket)
	case http.MethodPost:
		if q.Has("delete") {
			return s.deleteObjects(w, r, bucket, sig)
		}
		return errNotImplemented("this bucket operation")
	case http.MethodPut:
		return errNotImplemented("creating buckets over the API: buckets are declared in tana's configuration")
	case http.MethodDelete:
		return errNotImplemented("deleting buckets over the API: buckets are declared in tana's configuration")
	}
	return errNotImplemented("this method")
}

// unsupportedBucketQuery names a bucket sub-resource tana does not
// implement, so the client is told which one rather than being handed
// an empty listing it will misread as success.
func unsupportedBucketQuery(q url.Values) string {
	for _, name := range []string{
		"versioning", "versions", "lifecycle", "policy", "acl", "cors",
		"tagging", "replication", "encryption", "notification",
		"object-lock", "accelerate", "logging", "website", "requestPayment",
	} {
		if q.Has(name) {
			return "bucket " + name
		}
	}
	return ""
}

// objectOp handles requests addressed to a key.
func (s *Server) objectOp(w http.ResponseWriter, r *http.Request, bucket, key string, sig signature) *APIError {
	q := r.URL.Query()
	uploadID := q.Get("uploadId")

	switch r.Method {
	case http.MethodGet:
		if uploadID != "" {
			return s.listParts(w, bucket, key, uploadID)
		}
		return s.getObject(w, r, bucket, key, true)
	case http.MethodHead:
		return s.getObject(w, r, bucket, key, false)
	case http.MethodPut:
		if uploadID != "" {
			return s.uploadPart(w, r, bucket, uploadID, q.Get("partNumber"), sig)
		}
		if src := r.Header.Get("X-Amz-Copy-Source"); src != "" {
			return s.copyObject(w, r, bucket, key, src)
		}
		return s.putObject(w, r, bucket, key, sig)
	case http.MethodPost:
		if q.Has("uploads") {
			return s.createUpload(w, bucket, key)
		}
		if uploadID != "" {
			return s.completeUpload(w, r, bucket, key, uploadID)
		}
		return errNotImplemented("this object operation")
	case http.MethodDelete:
		if uploadID != "" {
			return s.abortUpload(w, bucket, uploadID)
		}
		return s.deleteObject(w, bucket, key)
	}
	return errNotImplemented("this method")
}

// listBuckets answers GET /. A caller sees only its own bucket:
// enumerating the store's tenants is not something a tenant may do.
func (s *Server) listBuckets(w http.ResponseWriter, sig signature) *APIError {
	var res ListAllMyBucketsResult
	res.Owner = Owner{ID: sig.creds.Bucket, DisplayName: sig.creds.Bucket}
	res.Buckets.Bucket = []BucketEntry{{
		Name:         sig.creds.Bucket,
		CreationDate: time.Unix(0, 0).UTC(),
	}}
	return writeXML(w, http.StatusOK, res)
}

// listObjects answers ListObjectsV2.
func (s *Server) listObjects(w http.ResponseWriter, r *http.Request, bucket string) *APIError {
	q := r.URL.Query()
	if v := q.Get("list-type"); v != "" && v != "2" {
		return errNotImplemented("ListObjects v1: use list-type=2")
	}
	prefix := q.Get("prefix")
	delimiter := q.Get("delimiter")
	encode := q.Get("encoding-type") == "url"

	maxKeys := defaultMaxKeys
	if v := q.Get("max-keys"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return errInvalidArgument("max-keys must be a non-negative integer")
		}
		maxKeys = min(n, maxMaxKeys)
	}

	// The continuation token is just the key to resume after, encoded
	// so it survives a round trip through a query string. The index is
	// ordered, so resuming is a seek rather than a rescan.
	after := q.Get("start-after")
	if tok := q.Get("continuation-token"); tok != "" {
		raw, err := base64.StdEncoding.DecodeString(tok)
		if err != nil {
			return errInvalidArgument("malformed continuation-token")
		}
		after = string(raw)
	}

	res := ListBucketResult{
		Name: bucket, Prefix: prefix, Delimiter: delimiter, MaxKeys: maxKeys,
		ContinuationToken: q.Get("continuation-token"), StartAfter: q.Get("start-after"),
	}
	if encode {
		res.EncodingType = "url"
	}

	seenPrefix := map[string]bool{}
	var last string
	stop := errors.New("page full")

	err := s.store.List(bucket, prefix, func(e index.Entry) error {
		if after != "" && e.Key <= after {
			return nil
		}
		if res.KeyCount >= maxKeys {
			res.IsTruncated = true
			return stop
		}
		last = e.Key

		// A delimiter turns the flat key space into something that
		// looks like directories, which is how every browser and
		// half the plugins expect to walk it.
		if delimiter != "" {
			rest := strings.TrimPrefix(e.Key, prefix)
			if i := strings.Index(rest, delimiter); i >= 0 {
				group := prefix + rest[:i+len(delimiter)]
				if !seenPrefix[group] {
					seenPrefix[group] = true
					res.CommonPrefixes = append(res.CommonPrefixes, CommonPrefix{Prefix: maybeEncode(group, encode)})
					res.KeyCount++
				}
				return nil
			}
		}
		res.Contents = append(res.Contents, ObjectEntry{
			Key:          maybeEncode(e.Key, encode),
			LastModified: e.ModTime.UTC(),
			ETag:         quoteETag(e.ETag),
			Size:         e.Size,
			StorageClass: storageClass,
		})
		res.KeyCount++
		return nil
	})
	if err != nil && !errors.Is(err, stop) {
		return s.fail("list", err)
	}
	if res.IsTruncated {
		res.NextContinuationToken = base64.StdEncoding.EncodeToString([]byte(last))
	}
	return writeXML(w, http.StatusOK, res)
}

// getObject serves GET and HEAD. HEAD takes the same path so the two
// can never disagree about an object's metadata.
func (s *Server) getObject(w http.ResponseWriter, r *http.Request, bucket, key string, body bool) *APIError {
	e, err := s.store.Head(bucket, key)
	if err != nil {
		return s.fail("head", err)
	}

	h := w.Header()
	h.Set("ETag", quoteETag(e.ETag))
	h.Set("Last-Modified", e.ModTime.UTC().Format(http.TimeFormat))
	h.Set("Accept-Ranges", "bytes")
	h.Set("x-amz-storage-class", storageClass)
	if ct := contentType(key); ct != "" {
		h.Set("Content-Type", ct)
	}

	if !body {
		h.Set("Content-Length", strconv.FormatInt(e.Size, 10))
		w.WriteHeader(http.StatusOK)
		return nil
	}

	_, rc, err := s.store.Get(bucket, key)
	if err != nil {
		return s.fail("get", err)
	}
	defer rc.Close()

	offset, length, partial, apiErr := parseRange(r.Header.Get("Range"), e.Size)
	if apiErr != nil {
		h.Set("Content-Range", fmt.Sprintf("bytes */%d", e.Size))
		return apiErr
	}
	if partial {
		if seeker, ok := rc.(io.Seeker); ok {
			if _, err := seeker.Seek(offset, io.SeekStart); err != nil {
				return s.fail("seek", err)
			}
		} else if _, err := io.CopyN(io.Discard, rc, offset); err != nil {
			return s.fail("seek", err)
		}
		h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", offset, offset+length-1, e.Size))
		h.Set("Content-Length", strconv.FormatInt(length, 10))
		w.WriteHeader(http.StatusPartialContent)
		io.CopyN(w, rc, length)
		return nil
	}

	h.Set("Content-Length", strconv.FormatInt(e.Size, 10))
	w.WriteHeader(http.StatusOK)
	io.Copy(w, rc)
	return nil
}

// putObject stores a whole object in one request.
func (s *Server) putObject(w http.ResponseWriter, r *http.Request, bucket, key string, sig signature) *APIError {
	if r.ContentLength > maxObjectSize {
		return errEntityTooLarge()
	}
	body, expectHash, apiErr := s.requestBody(r, sig)
	if apiErr != nil {
		return apiErr
	}

	md5Check, body := wrapMD5(r.Header.Get("Content-MD5"), body)

	e, err := s.store.PutWith(bucket, key, body, store.PutOptions{
		MTime:      time.Now().UTC(),
		ExpectHash: expectHash,
	})
	if err != nil {
		return s.fail("put", err)
	}
	if md5Check != nil && !md5Check.matches() {
		return errInvalidDigest()
	}

	w.Header().Set("ETag", quoteETag(e.ETag))
	w.WriteHeader(http.StatusOK)
	return nil
}

// copyObject performs a server-side copy. With content addressing this
// costs a reference count and nothing else: the bytes are already
// there, and both keys point at the same blob.
func (s *Server) copyObject(w http.ResponseWriter, r *http.Request, bucket, key, source string) *APIError {
	srcBucket, srcKey := splitPath("/" + strings.TrimPrefix(source, "/"))
	if unescaped, err := url.PathUnescape(srcKey); err == nil {
		srcKey = unescaped
	}
	if srcBucket == "" || srcKey == "" {
		return errInvalidArgument("malformed x-amz-copy-source")
	}
	if srcBucket != bucket {
		return errAccessDenied()
	}

	_, rc, err := s.store.Get(srcBucket, srcKey)
	if err != nil {
		return s.fail("copy source", err)
	}
	defer rc.Close()

	e, err := s.store.Put(bucket, key, rc, time.Now().UTC())
	if err != nil {
		return s.fail("copy", err)
	}
	return writeXML(w, http.StatusOK, CopyObjectResult{
		LastModified: e.ModTime.UTC(),
		ETag:         quoteETag(e.ETag),
	})
}

// deleteObject removes one key.
func (s *Server) deleteObject(w http.ResponseWriter, bucket, key string) *APIError {
	if err := s.store.Delete(bucket, key); err != nil {
		return s.fail("delete", err)
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

// deleteObjects handles a batch delete.
func (s *Server) deleteObjects(w http.ResponseWriter, r *http.Request, bucket string, sig signature) *APIError {
	body, _, apiErr := s.requestBody(r, sig)
	if apiErr != nil {
		return apiErr
	}
	raw, err := io.ReadAll(io.LimitReader(body, 8<<20))
	if err != nil {
		return errInvalidRequest("could not read the request body")
	}
	var req DeleteRequest
	if err := xml.Unmarshal(raw, &req); err != nil {
		return errMalformedXML()
	}
	if len(req.Objects) > maxDeleteBatch {
		return errInvalidArgument(fmt.Sprintf("a batch delete may list at most %d keys", maxDeleteBatch))
	}

	var res DeleteResult
	for _, o := range req.Objects {
		if err := s.store.Delete(bucket, o.Key); err != nil {
			res.Errors = append(res.Errors, DeleteError{
				Key: o.Key, Code: "InternalError", Message: err.Error(),
			})
			continue
		}
		if !req.Quiet {
			res.Deleted = append(res.Deleted, DeletedObject{Key: o.Key})
		}
	}
	return writeXML(w, http.StatusOK, res)
}

// createUpload starts a multipart upload.
func (s *Server) createUpload(w http.ResponseWriter, bucket, key string) *APIError {
	u, err := s.store.CreateUpload(bucket, key)
	if err != nil {
		return s.fail("create upload", err)
	}
	return writeXML(w, http.StatusOK, InitiateMultipartUploadResult{
		Bucket: bucket, Key: key, UploadID: u.ID,
	})
}

// uploadPart stores one part of a multipart upload.
func (s *Server) uploadPart(w http.ResponseWriter, r *http.Request, bucket, uploadID, partNumber string, sig signature) *APIError {
	n, err := strconv.Atoi(partNumber)
	if err != nil {
		return errInvalidArgument("partNumber must be an integer")
	}
	body, _, apiErr := s.requestBody(r, sig)
	if apiErr != nil {
		return apiErr
	}
	part, err := s.store.UploadPart(bucket, uploadID, n, body)
	if err != nil {
		return s.fail("upload part", err)
	}
	w.Header().Set("ETag", quoteETag(part.ETag))
	w.WriteHeader(http.StatusOK)
	return nil
}

// completeUpload assembles a multipart upload.
func (s *Server) completeUpload(w http.ResponseWriter, r *http.Request, bucket, key, uploadID string) *APIError {
	raw, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	if err != nil {
		return errInvalidRequest("could not read the request body")
	}
	var req CompleteMultipartUpload
	if err := xml.Unmarshal(raw, &req); err != nil {
		return errMalformedXML()
	}
	parts := make([]store.CompletedPart, 0, len(req.Parts))
	for _, p := range req.Parts {
		parts = append(parts, store.CompletedPart{Number: p.PartNumber, ETag: p.ETag})
	}

	e, err := s.store.CompleteUpload(bucket, uploadID, parts)
	if err != nil {
		return s.fail("complete upload", err)
	}
	return writeXML(w, http.StatusOK, CompleteMultipartUploadResult{
		Location: "/" + bucket + "/" + key,
		Bucket:   bucket,
		Key:      key,
		ETag:     quoteETag(e.ETag),
	})
}

// abortUpload discards an upload in progress.
func (s *Server) abortUpload(w http.ResponseWriter, bucket, uploadID string) *APIError {
	if err := s.store.AbortUpload(bucket, uploadID); err != nil {
		return s.fail("abort upload", err)
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

// listParts lists what a multipart upload has received so far.
func (s *Server) listParts(w http.ResponseWriter, bucket, key, uploadID string) *APIError {
	u, numbers, err := s.store.ListParts(bucket, uploadID)
	if err != nil {
		return s.fail("list parts", err)
	}
	res := ListPartsResult{Bucket: bucket, Key: key, UploadID: uploadID, MaxParts: len(numbers)}
	for _, n := range numbers {
		p := u.Parts[n]
		res.Parts = append(res.Parts, PartInfo{
			PartNumber: n, LastModified: p.Uploaded, ETag: quoteETag(p.ETag), Size: p.Size,
		})
	}
	return writeXML(w, http.StatusOK, res)
}

// listUploads lists the multipart uploads in progress.
func (s *Server) listUploads(w http.ResponseWriter, bucket string) *APIError {
	ups, err := s.store.ListUploads(bucket)
	if err != nil {
		return s.fail("list uploads", err)
	}
	res := ListMultipartUploadsResult{Bucket: bucket}
	for _, u := range ups {
		res.Uploads = append(res.Uploads, UploadInfo{Key: u.Key, UploadID: u.ID, Initiated: u.Initiated})
	}
	return writeXML(w, http.StatusOK, res)
}

// requestBody returns the payload reader, unwrapping aws-chunked
// framing, and the content hash the client committed to (empty when it
// declined to declare one).
func (s *Server) requestBody(r *http.Request, sig signature) (io.Reader, string, *APIError) {
	if sig.streaming() {
		return newChunkedReader(r.Body, sig, s.region), "", nil
	}
	switch sig.payload {
	case "", unsignedPayload:
		return r.Body, "", nil
	default:
		// A hex digest is a promise about the body. It is checked after
		// storing, against the hash the store computed anyway.
		if len(sig.payload) == 64 {
			return r.Body, sig.payload, nil
		}
		return r.Body, "", nil
	}
}

// fail maps a store error onto the S3 error the client expects.
func (s *Server) fail(op string, err error) *APIError {
	var noBucket store.ErrNoSuchBucket
	var noKey store.ErrNoSuchKey
	var noUpload store.ErrNoSuchUpload
	var badPart store.ErrInvalidPart
	var digest store.ErrDigestMismatch
	switch {
	case errors.As(err, &noBucket):
		return errNoSuchBucket()
	case errors.As(err, &noKey):
		return errNoSuchKey()
	case errors.As(err, &noUpload):
		return errNoSuchUpload()
	case errors.As(err, &badPart):
		return errInvalidPart()
	case errors.As(err, &digest):
		return errInvalidDigest()
	}
	// Anything unclassified is ours, not the client's: log it with the
	// detail and hand back the opaque error S3 defines.
	s.svcLog.Error("request failed", "op", op, "error", err)
	return errInternal()
}

// writeError renders an S3 error document.
func (s *Server) writeError(w http.ResponseWriter, r *http.Request, e *APIError) {
	e.Resource = r.URL.Path
	// A HEAD carries no body, so the status is all the client gets.
	if r.Method == http.MethodHead {
		w.WriteHeader(e.Status())
		return
	}
	writeXML(w, e.Status(), e)
}

// writeXML renders a document with the XML declaration S3 clients
// expect ahead of it.
func writeXML(w http.ResponseWriter, status int, v any) *APIError {
	body, err := xml.Marshal(v)
	if err != nil {
		return errInternal()
	}
	out := append([]byte(xml.Header), body...)
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("Content-Length", strconv.Itoa(len(out)))
	w.WriteHeader(status)
	w.Write(out)
	return nil
}

// splitPath separates the bucket from the key in a path-style URL.
func splitPath(p string) (bucket, key string) {
	trimmed := strings.TrimPrefix(path.Clean("/"+strings.TrimPrefix(p, "/")), "/")
	if trimmed == "." || trimmed == "" {
		return "", ""
	}
	bucket, key, _ = strings.Cut(trimmed, "/")
	return bucket, key
}

// parseRange interprets a Range header. Only the single-range form is
// supported, which is the only one any S3 client sends.
func parseRange(header string, size int64) (offset, length int64, partial bool, apiErr *APIError) {
	if header == "" {
		return 0, size, false, nil
	}
	spec, ok := strings.CutPrefix(header, "bytes=")
	if !ok || strings.Contains(spec, ",") {
		return 0, 0, false, errInvalidRange()
	}
	first, last, found := strings.Cut(spec, "-")
	if !found {
		return 0, 0, false, errInvalidRange()
	}

	switch {
	case first == "" && last == "":
		return 0, 0, false, errInvalidRange()
	case first == "": // suffix: the final N bytes
		n, err := strconv.ParseInt(last, 10, 64)
		if err != nil || n <= 0 {
			return 0, 0, false, errInvalidRange()
		}
		if n > size {
			n = size
		}
		return size - n, n, true, nil
	default:
		start, err := strconv.ParseInt(first, 10, 64)
		if err != nil || start < 0 || start >= size {
			return 0, 0, false, errInvalidRange()
		}
		end := size - 1
		if last != "" {
			if end, err = strconv.ParseInt(last, 10, 64); err != nil {
				return 0, 0, false, errInvalidRange()
			}
		}
		if end >= size {
			end = size - 1
		}
		if end < start {
			return 0, 0, false, errInvalidRange()
		}
		return start, end - start + 1, true, nil
	}
}

// contentType guesses from the key's extension. WordPress serves media
// through a web server that does the same, so agreeing with it keeps a
// CDN in front of the store from caching the wrong type.
func contentType(key string) string {
	if ct := mime.TypeByExtension(path.Ext(key)); ct != "" {
		return ct
	}
	return "application/octet-stream"
}

// quoteETag wraps a tag in the quotes the S3 API puts around it.
func quoteETag(tag string) string {
	if tag == "" {
		return ""
	}
	if strings.HasPrefix(tag, `"`) {
		return tag
	}
	return `"` + tag + `"`
}

// maybeEncode URL-encodes a key when the client asked for it, which is
// how a key containing characters XML cannot carry is returned.
func maybeEncode(s string, encode bool) string {
	if !encode {
		return s
	}
	return url.QueryEscape(s)
}

// md5Checker verifies a client-supplied Content-MD5.
type md5Checker struct {
	want string
	got  interface{ Sum([]byte) []byte }
}

func (m *md5Checker) matches() bool {
	return base64.StdEncoding.EncodeToString(m.got.Sum(nil)) == m.want ||
		hex.EncodeToString(m.got.Sum(nil)) == m.want
}

// wrapMD5 tees the body through an md5 hash when the client declared
// one. Returns a nil checker when it did not.
func wrapMD5(header string, body io.Reader) (*md5Checker, io.Reader) {
	if header == "" {
		return nil, body
	}
	h := md5.New()
	return &md5Checker{want: header, got: h}, io.TeeReader(body, h)
}

// recorder captures the status and byte count for the access log.
type recorder struct {
	http.ResponseWriter
	status  int
	written int64
	wrote   bool
}

func (r *recorder) WriteHeader(status int) {
	if r.wrote {
		return
	}
	r.wrote = true
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *recorder) Write(p []byte) (int, error) {
	if !r.wrote {
		r.WriteHeader(http.StatusOK)
	}
	n, err := r.ResponseWriter.Write(p)
	r.written += int64(n)
	return n, err
}
