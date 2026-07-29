package s3

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

func put(t *testing.T, c *client, key, content string) *http.Response {
	t.Helper()
	return c.do(request{method: http.MethodPut, path: "/" + testBucket + "/" + key, body: []byte(content)})
}

func TestPutGetHeadDelete(t *testing.T) {
	h := newHarness(t)
	const content = "pretend this is a jpeg"

	resp := put(t, h.client, "2026/07/foto.jpg", content)
	expectStatus(t, resp, http.StatusOK)
	etag := resp.Header.Get("ETag")
	if etag == "" || !strings.HasPrefix(etag, `"`) {
		t.Errorf("ETag = %q, want a quoted tag", etag)
	}

	resp = h.do(request{method: http.MethodGet, path: "/" + testBucket + "/2026/07/foto.jpg"})
	body := expectStatus(t, resp, http.StatusOK)
	if string(body) != content {
		t.Errorf("body = %q", body)
	}
	if got := resp.Header.Get("Content-Type"); got != "image/jpeg" {
		t.Errorf("Content-Type = %q, want image/jpeg", got)
	}
	if got := resp.Header.Get("ETag"); got != etag {
		t.Errorf("GET ETag %q disagrees with PUT %q", got, etag)
	}
	if resp.Header.Get("Last-Modified") == "" {
		t.Error("missing Last-Modified")
	}

	resp = h.do(request{method: http.MethodHead, path: "/" + testBucket + "/2026/07/foto.jpg"})
	expectStatus(t, resp, http.StatusOK)
	if got := resp.ContentLength; got != int64(len(content)) {
		t.Errorf("HEAD Content-Length = %d, want %d", got, len(content))
	}

	resp = h.do(request{method: http.MethodDelete, path: "/" + testBucket + "/2026/07/foto.jpg"})
	expectStatus(t, resp, http.StatusNoContent)

	resp = h.do(request{method: http.MethodGet, path: "/" + testBucket + "/2026/07/foto.jpg"})
	expectError(t, resp, http.StatusNotFound, "NoSuchKey")
}

func TestKeysWithAwkwardCharacters(t *testing.T) {
	h := newHarness(t)
	// WordPress produces keys with spaces, accents and plus signs, and
	// each of them is encoded differently by the signature, the URL and
	// the XML listing. Getting one of the three wrong is the classic
	// way an S3 implementation half-works.
	for _, key := range []string{
		"2026/07/foto con spazi.jpg",
		"2026/07/città-perù.png",
		"2026/07/a+b=c.webp",
		"2026/07/100%25-off.jpg",
	} {
		content := "content of " + key
		resp := h.do(request{
			method: http.MethodPut,
			path:   "/" + testBucket + "/" + key,
			body:   []byte(content),
		})
		expectStatus(t, resp, http.StatusOK)

		resp = h.do(request{method: http.MethodGet, path: "/" + testBucket + "/" + key})
		body := expectStatus(t, resp, http.StatusOK)
		if string(body) != content {
			t.Errorf("key %q read back %q", key, body)
		}
	}
}

func TestRangeRequests(t *testing.T) {
	h := newHarness(t)
	put(t, h.client, "a.txt", "0123456789")

	cases := []struct {
		header string
		want   string
		status int
	}{
		{"bytes=0-3", "0123", http.StatusPartialContent},
		{"bytes=4-", "456789", http.StatusPartialContent},
		{"bytes=-3", "789", http.StatusPartialContent},
		{"bytes=8-100", "89", http.StatusPartialContent}, // clamped to the end
		{"", "0123456789", http.StatusOK},
	}
	for _, c := range cases {
		req := request{method: http.MethodGet, path: "/" + testBucket + "/a.txt"}
		if c.header != "" {
			req.header = http.Header{"Range": {c.header}}
		}
		resp := h.do(req)
		body := expectStatus(t, resp, c.status)
		if string(body) != c.want {
			t.Errorf("Range %q = %q, want %q", c.header, body, c.want)
		}
	}

	resp := h.do(request{
		method: http.MethodGet, path: "/" + testBucket + "/a.txt",
		header: http.Header{"Range": {"bytes=50-60"}},
	})
	expectError(t, resp, http.StatusRequestedRangeNotSatisfiable, "InvalidRange")
}

func TestListObjectsV2(t *testing.T) {
	h := newHarness(t)
	for _, k := range []string{
		"2025/12/old.jpg", "2026/07/a.jpg", "2026/07/b.jpg", "2026/08/c.jpg",
	} {
		put(t, h.client, k, k)
	}

	resp := h.do(request{
		method: http.MethodGet, path: "/" + testBucket,
		query: url.Values{"list-type": {"2"}},
	})
	var res ListBucketResult
	decodeXML(t, expectStatus(t, resp, http.StatusOK), &res)
	if res.KeyCount != 4 || res.IsTruncated {
		t.Fatalf("listing = %d keys, truncated %v", res.KeyCount, res.IsTruncated)
	}
	if res.Contents[0].Key != "2025/12/old.jpg" {
		t.Errorf("listing is not in key order: %+v", res.Contents)
	}
	if res.Contents[0].ETag == "" || res.Contents[0].StorageClass != storageClass {
		t.Errorf("listing entry is incomplete: %+v", res.Contents[0])
	}

	// Prefix.
	resp = h.do(request{
		method: http.MethodGet, path: "/" + testBucket,
		query: url.Values{"list-type": {"2"}, "prefix": {"2026/07/"}},
	})
	res = ListBucketResult{}
	decodeXML(t, expectStatus(t, resp, http.StatusOK), &res)
	if res.KeyCount != 2 {
		t.Errorf("prefix listing = %d keys, want 2", res.KeyCount)
	}
}

func TestListObjectsDelimiter(t *testing.T) {
	h := newHarness(t)
	for _, k := range []string{"2026/07/a.jpg", "2026/07/b.jpg", "2026/08/c.jpg", "top.jpg"} {
		put(t, h.client, k, k)
	}

	resp := h.do(request{
		method: http.MethodGet, path: "/" + testBucket,
		query: url.Values{"list-type": {"2"}, "delimiter": {"/"}},
	})
	var res ListBucketResult
	decodeXML(t, expectStatus(t, resp, http.StatusOK), &res)

	if len(res.CommonPrefixes) != 1 || res.CommonPrefixes[0].Prefix != "2026/" {
		t.Errorf("common prefixes = %+v, want [2026/]", res.CommonPrefixes)
	}
	if len(res.Contents) != 1 || res.Contents[0].Key != "top.jpg" {
		t.Errorf("contents = %+v, want only top.jpg", res.Contents)
	}
}

func TestListObjectsPagination(t *testing.T) {
	h := newHarness(t)
	for i := 0; i < 10; i++ {
		put(t, h.client, fmt.Sprintf("k%02d.jpg", i), "x")
	}

	var seen []string
	token := ""
	for page := 0; page < 10; page++ {
		q := url.Values{"list-type": {"2"}, "max-keys": {"3"}}
		if token != "" {
			q.Set("continuation-token", token)
		}
		resp := h.do(request{method: http.MethodGet, path: "/" + testBucket, query: q})
		var res ListBucketResult
		decodeXML(t, expectStatus(t, resp, http.StatusOK), &res)

		for _, c := range res.Contents {
			seen = append(seen, c.Key)
		}
		if !res.IsTruncated {
			break
		}
		if res.NextContinuationToken == "" {
			t.Fatal("truncated listing without a continuation token")
		}
		token = res.NextContinuationToken
	}

	if len(seen) != 10 {
		t.Fatalf("paged through %d keys, want 10: %v", len(seen), seen)
	}
	for i, k := range seen {
		if want := fmt.Sprintf("k%02d.jpg", i); k != want {
			t.Fatalf("page order broke at %d: %q, want %q", i, k, want)
		}
	}
}

func TestListBucketsAndHeadBucket(t *testing.T) {
	h := newHarness(t)

	resp := h.do(request{method: http.MethodGet, path: "/"})
	var res ListAllMyBucketsResult
	decodeXML(t, expectStatus(t, resp, http.StatusOK), &res)
	// A tenant sees its own bucket and no other: the store's tenant
	// list is not a tenant's business.
	if len(res.Buckets.Bucket) != 1 || res.Buckets.Bucket[0].Name != testBucket {
		t.Errorf("ListBuckets = %+v, want only %s", res.Buckets.Bucket, testBucket)
	}

	resp = h.do(request{method: http.MethodHead, path: "/" + testBucket})
	expectStatus(t, resp, http.StatusOK)
}

func TestBatchDelete(t *testing.T) {
	h := newHarness(t)
	for _, k := range []string{"a.jpg", "b.jpg", "c.jpg"} {
		put(t, h.client, k, k)
	}

	body := []byte(`<Delete><Object><Key>a.jpg</Key></Object><Object><Key>b.jpg</Key></Object></Delete>`)
	resp := h.do(request{
		method: http.MethodPost, path: "/" + testBucket,
		query: url.Values{"delete": {""}}, body: body,
	})
	var res DeleteResult
	decodeXML(t, expectStatus(t, resp, http.StatusOK), &res)
	if len(res.Deleted) != 2 || len(res.Errors) != 0 {
		t.Fatalf("delete result = %+v", res)
	}

	resp = h.do(request{method: http.MethodGet, path: "/" + testBucket + "/c.jpg"})
	expectStatus(t, resp, http.StatusOK)
	resp = h.do(request{method: http.MethodGet, path: "/" + testBucket + "/a.jpg"})
	expectError(t, resp, http.StatusNotFound, "NoSuchKey")
}

func TestCopyObject(t *testing.T) {
	h := newHarness(t)
	put(t, h.client, "source.jpg", "the bytes")

	resp := h.do(request{
		method: http.MethodPut, path: "/" + testBucket + "/dest.jpg",
		header: http.Header{"X-Amz-Copy-Source": {"/" + testBucket + "/source.jpg"}},
	})
	var res CopyObjectResult
	decodeXML(t, expectStatus(t, resp, http.StatusOK), &res)
	if res.ETag == "" {
		t.Error("copy returned no ETag")
	}

	resp = h.do(request{method: http.MethodGet, path: "/" + testBucket + "/dest.jpg"})
	if got := string(expectStatus(t, resp, http.StatusOK)); got != "the bytes" {
		t.Errorf("copy read back %q", got)
	}
	// Both keys point at one blob: a copy costs a reference, not bytes.
	count, _, err := h.store.Blobs().Usage()
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("blobs on disk after a copy = %d, want 1", count)
	}
}

func TestMultipartUpload(t *testing.T) {
	h := newHarness(t)
	key := "big.bin"

	resp := h.do(request{
		method: http.MethodPost, path: "/" + testBucket + "/" + key,
		query: url.Values{"uploads": {""}},
	})
	var created InitiateMultipartUploadResult
	decodeXML(t, expectStatus(t, resp, http.StatusOK), &created)
	if created.UploadID == "" {
		t.Fatal("no upload id")
	}

	parts := []string{strings.Repeat("a", 1024), strings.Repeat("b", 512)}
	var etags []string
	for i, p := range parts {
		resp := h.do(request{
			method: http.MethodPut, path: "/" + testBucket + "/" + key,
			query: url.Values{"uploadId": {created.UploadID}, "partNumber": {fmt.Sprint(i + 1)}},
			body:  []byte(p),
		})
		expectStatus(t, resp, http.StatusOK)
		tag := resp.Header.Get("ETag")
		if tag == "" {
			t.Fatalf("part %d returned no ETag", i+1)
		}
		etags = append(etags, tag)
	}

	// The parts are visible before completion.
	resp = h.do(request{
		method: http.MethodGet, path: "/" + testBucket + "/" + key,
		query: url.Values{"uploadId": {created.UploadID}},
	})
	var listed ListPartsResult
	decodeXML(t, expectStatus(t, resp, http.StatusOK), &listed)
	if len(listed.Parts) != 2 {
		t.Errorf("listed %d parts, want 2", len(listed.Parts))
	}

	var body strings.Builder
	body.WriteString("<CompleteMultipartUpload>")
	for i, tag := range etags {
		fmt.Fprintf(&body, "<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>", i+1, tag)
	}
	body.WriteString("</CompleteMultipartUpload>")

	resp = h.do(request{
		method: http.MethodPost, path: "/" + testBucket + "/" + key,
		query: url.Values{"uploadId": {created.UploadID}}, body: []byte(body.String()),
	})
	var done CompleteMultipartUploadResult
	decodeXML(t, expectStatus(t, resp, http.StatusOK), &done)
	// A multipart ETag ends in the part count; clients rely on the
	// suffix to know an object was not uploaded whole.
	if !strings.HasSuffix(strings.Trim(done.ETag, `"`), "-2") {
		t.Errorf("multipart ETag = %s, want a -2 suffix", done.ETag)
	}

	resp = h.do(request{method: http.MethodGet, path: "/" + testBucket + "/" + key})
	got := expectStatus(t, resp, http.StatusOK)
	if string(got) != parts[0]+parts[1] {
		t.Errorf("assembled object is %d bytes, want %d", len(got), len(parts[0])+len(parts[1]))
	}
}

func TestMultipartAbortReleasesParts(t *testing.T) {
	h := newHarness(t)
	key := "abandoned.bin"

	resp := h.do(request{
		method: http.MethodPost, path: "/" + testBucket + "/" + key,
		query: url.Values{"uploads": {""}},
	})
	var created InitiateMultipartUploadResult
	decodeXML(t, expectStatus(t, resp, http.StatusOK), &created)

	resp = h.do(request{
		method: http.MethodPut, path: "/" + testBucket + "/" + key,
		query: url.Values{"uploadId": {created.UploadID}, "partNumber": {"1"}},
		body:  []byte("a part nobody will finish"),
	})
	expectStatus(t, resp, http.StatusOK)

	resp = h.do(request{
		method: http.MethodDelete, path: "/" + testBucket + "/" + key,
		query: url.Values{"uploadId": {created.UploadID}},
	})
	expectStatus(t, resp, http.StatusNoContent)

	// The part's blob is unreferenced now, so the collector takes it
	// once the grace period has run.
	st, err := h.store.GC(time.Now().Add(100 * time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if st.Collected != 1 {
		t.Errorf("collected %d blob(s) after an abort, want 1", st.Collected)
	}

	resp = h.do(request{
		method: http.MethodGet, path: "/" + testBucket + "/" + key,
		query: url.Values{"uploadId": {created.UploadID}},
	})
	expectError(t, resp, http.StatusNotFound, "NoSuchUpload")
}

func TestMultipartRejectsWrongPartETag(t *testing.T) {
	h := newHarness(t)
	key := "big.bin"
	resp := h.do(request{
		method: http.MethodPost, path: "/" + testBucket + "/" + key,
		query: url.Values{"uploads": {""}},
	})
	var created InitiateMultipartUploadResult
	decodeXML(t, expectStatus(t, resp, http.StatusOK), &created)

	resp = h.do(request{
		method: http.MethodPut, path: "/" + testBucket + "/" + key,
		query: url.Values{"uploadId": {created.UploadID}, "partNumber": {"1"}},
		body:  []byte("real content"),
	})
	expectStatus(t, resp, http.StatusOK)

	body := `<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>"deadbeef"</ETag></Part></CompleteMultipartUpload>`
	resp = h.do(request{
		method: http.MethodPost, path: "/" + testBucket + "/" + key,
		query: url.Values{"uploadId": {created.UploadID}}, body: []byte(body),
	})
	expectError(t, resp, http.StatusBadRequest, "InvalidPart")
}

func TestPresignedGet(t *testing.T) {
	h := newHarness(t)
	put(t, h.client, "protected.pdf", "the invoice")

	u := h.presign(http.MethodGet, "/"+testBucket+"/protected.pdf", 5*time.Minute)
	resp, err := http.Get(u)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(expectStatus(t, resp, http.StatusOK)); got != "the invoice" {
		t.Errorf("presigned GET returned %q", got)
	}

	// This is the mechanism WooCommerce downloads will lean on, so a
	// link whose window has closed must fail shut.
	expired := h.presignAt(http.MethodGet, "/"+testBucket+"/protected.pdf",
		time.Minute, time.Now().UTC().Add(-time.Hour))
	resp, err = http.Get(expired)
	if err != nil {
		t.Fatal(err)
	}
	expectError(t, resp, http.StatusForbidden, "RequestTimeTooSkewed")

	// A presigned URL is scoped to one method: a read link must not
	// also authorize a write.
	readLink := h.presign(http.MethodGet, "/"+testBucket+"/protected.pdf", 5*time.Minute)
	req, err := http.NewRequest(http.MethodPut, readLink, strings.NewReader("overwrite"))
	if err != nil {
		t.Fatal(err)
	}
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	expectError(t, resp, http.StatusForbidden, "SignatureDoesNotMatch")
}

func TestUnauthenticatedRequestIsRefused(t *testing.T) {
	h := newHarness(t)
	put(t, h.client, "a.jpg", "secret")

	resp, err := http.Get(h.base + "/" + testBucket + "/a.jpg")
	if err != nil {
		t.Fatal(err)
	}
	expectError(t, resp, http.StatusForbidden, "MissingSecurityHeader")
}

func TestTamperedSignatureIsRefused(t *testing.T) {
	h := newHarness(t)
	resp := h.do(request{
		method: http.MethodPut, path: "/" + testBucket + "/a.jpg",
		body: []byte("x"), tamper: true,
	})
	expectError(t, resp, http.StatusForbidden, "SignatureDoesNotMatch")
}

func TestUnknownAccessKeyIsRefused(t *testing.T) {
	h := newHarness(t)
	c := h.as("NOSUCHKEY00000000000", testSK)
	resp := c.do(request{method: http.MethodGet, path: "/" + testBucket + "/a.jpg"})
	expectError(t, resp, http.StatusForbidden, "InvalidAccessKeyId")
}

func TestCredentialsCannotReachAnotherBucket(t *testing.T) {
	h := newHarness(t)
	// The tenant boundary: valid credentials, someone else's bucket.
	resp := h.do(request{method: http.MethodGet, path: "/other-site/a.jpg"})
	expectError(t, resp, http.StatusForbidden, "AccessDenied")

	resp = h.do(request{
		method: http.MethodPut, path: "/other-site/a.jpg", body: []byte("intrusion"),
	})
	expectError(t, resp, http.StatusForbidden, "AccessDenied")
}

func TestSkewedClockIsRefused(t *testing.T) {
	h := newHarness(t)
	resp := h.do(request{
		method: http.MethodGet, path: "/" + testBucket + "/a.jpg",
		skew: 30 * time.Minute,
	})
	expectError(t, resp, http.StatusForbidden, "RequestTimeTooSkewed")
}

func TestDeclaredContentHashIsEnforced(t *testing.T) {
	h := newHarness(t)
	// A body that does not match the hash the client signed must be
	// rejected: the signature covers the hash, so accepting the body
	// anyway would make the signature decorative.
	c := h.client
	req := request{method: http.MethodPut, path: "/" + testBucket + "/a.jpg", body: []byte("declared")}
	u := c.base + req.path
	r, err := http.NewRequest(req.method, u, strings.NewReader("actually different"))
	if err != nil {
		t.Fatal(err)
	}
	sum := hashOf("declared")
	c.sign(r, sum, time.Now().UTC(), nil)
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	expectError(t, resp, http.StatusBadRequest, "BadDigest")
}

func TestUnsignedPayloadIsAccepted(t *testing.T) {
	h := newHarness(t)
	// Clients that stream without hashing send UNSIGNED-PAYLOAD; the
	// request stays authenticated, the body simply is not covered.
	resp := h.do(request{
		method: http.MethodPut, path: "/" + testBucket + "/a.jpg",
		body: []byte("streamed"), unsigned: true,
	})
	expectStatus(t, resp, http.StatusOK)

	resp = h.do(request{method: http.MethodGet, path: "/" + testBucket + "/a.jpg"})
	if got := string(expectStatus(t, resp, http.StatusOK)); got != "streamed" {
		t.Errorf("read back %q", got)
	}
}

func TestUnimplementedFeaturesSaySo(t *testing.T) {
	h := newHarness(t)
	for _, q := range []string{"versioning", "lifecycle", "policy", "object-lock", "encryption"} {
		resp := h.do(request{
			method: http.MethodGet, path: "/" + testBucket,
			query: url.Values{q: {""}},
		})
		// Silence would be read as "no policy configured"; a client
		// deserves to know the difference.
		expectError(t, resp, http.StatusNotImplemented, "NotImplemented")
	}
}

func TestListObjectsV1IsRefusedExplicitly(t *testing.T) {
	h := newHarness(t)
	resp := h.do(request{
		method: http.MethodGet, path: "/" + testBucket,
		query: url.Values{"list-type": {"1"}},
	})
	expectError(t, resp, http.StatusNotImplemented, "NotImplemented")
}

func TestMissingKeyOnHeadHasNoBody(t *testing.T) {
	h := newHarness(t)
	resp := h.do(request{method: http.MethodHead, path: "/" + testBucket + "/missing.jpg"})
	body := readBody(t, resp)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
	if len(body) != 0 {
		t.Errorf("HEAD returned a %d byte body", len(body))
	}
}
