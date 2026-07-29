package s3

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// publicHarness has a bucket that serves media anonymously, with the
// WooCommerce download directory carved out.
func publicHarness(t *testing.T) *harness {
	t.Helper()
	h := newHarness(t)
	b, _ := h.store.Bucket(testBucket)
	b.PublicRead = true
	b.Protected = []string{"woocommerce_uploads/**"}
	h.store.Configure(storeConfigWith(b))
	return h
}

func TestPublicReadServesMedia(t *testing.T) {
	h := publicHarness(t)
	put(t, h.client, "2026/07/foto.jpg", "a public photo")

	// No credentials at all: what a browser or a CDN sends.
	resp, err := http.Get(h.base + "/" + testBucket + "/2026/07/foto.jpg")
	if err != nil {
		t.Fatal(err)
	}
	if got := string(expectStatus(t, resp, http.StatusOK)); got != "a public photo" {
		t.Errorf("body = %q", got)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "image/jpeg" {
		t.Errorf("Content-Type = %q", ct)
	}

	resp, err = http.Head(h.base + "/" + testBucket + "/2026/07/foto.jpg")
	if err != nil {
		t.Fatal(err)
	}
	expectStatus(t, resp, http.StatusOK)
}

func TestProtectedPrefixIsNeverPublic(t *testing.T) {
	h := publicHarness(t)
	put(t, h.client, "woocommerce_uploads/2026/07/manual.pdf", "a paid download")

	// This is the whole point of the protected list: same bucket, same
	// public_read flag, and it must still refuse.
	resp, err := http.Get(h.base + "/" + testBucket + "/woocommerce_uploads/2026/07/manual.pdf")
	if err != nil {
		t.Fatal(err)
	}
	expectError(t, resp, http.StatusForbidden, "MissingSecurityHeader")

	// A presigned link still works: that is how WooCommerce hands a
	// customer their file.
	u := h.presign(http.MethodGet, "/"+testBucket+"/woocommerce_uploads/2026/07/manual.pdf", time.Minute)
	resp, err = http.Get(u)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(expectStatus(t, resp, http.StatusOK)); got != "a paid download" {
		t.Errorf("presigned download returned %q", got)
	}
}

func TestPublicReadDoesNotAllowWrites(t *testing.T) {
	h := publicHarness(t)
	req, err := http.NewRequest(http.MethodPut,
		h.base+"/"+testBucket+"/anyone.jpg", strings.NewReader("uploaded by a stranger"))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	expectError(t, resp, http.StatusForbidden, "MissingSecurityHeader")

	req, err = http.NewRequest(http.MethodDelete, h.base+"/"+testBucket+"/anyone.jpg", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	expectError(t, resp, http.StatusForbidden, "MissingSecurityHeader")
}

func TestPublicReadDoesNotAllowListing(t *testing.T) {
	h := publicHarness(t)
	put(t, h.client, "2026/07/foto.jpg", "x")

	// Serving a file anonymously is not the same as handing out the
	// index of every file there is.
	resp, err := http.Get(h.base + "/" + testBucket + "?list-type=2")
	if err != nil {
		t.Fatal(err)
	}
	expectError(t, resp, http.StatusForbidden, "MissingSecurityHeader")

	resp, err = http.Get(h.base + "/")
	if err != nil {
		t.Fatal(err)
	}
	expectError(t, resp, http.StatusForbidden, "MissingSecurityHeader")
}

func TestBadSignatureIsNotDowngradedToAnonymous(t *testing.T) {
	h := publicHarness(t)
	put(t, h.client, "2026/07/foto.jpg", "x")

	// A request that brings a broken signature must fail, not quietly
	// succeed through the public path. Otherwise a wrong signature is
	// better than no signature, which is absurd.
	resp := h.do(request{
		method: http.MethodGet, path: "/" + testBucket + "/2026/07/foto.jpg", tamper: true,
	})
	expectError(t, resp, http.StatusForbidden, "SignatureDoesNotMatch")
}

func TestPrivateBucketStaysPrivate(t *testing.T) {
	h := newHarness(t) // public_read not set
	put(t, h.client, "2026/07/foto.jpg", "private media")

	resp, err := http.Get(h.base + "/" + testBucket + "/2026/07/foto.jpg")
	if err != nil {
		t.Fatal(err)
	}
	expectError(t, resp, http.StatusForbidden, "MissingSecurityHeader")
}

func TestPublicReadOfMissingKey(t *testing.T) {
	h := publicHarness(t)
	resp, err := http.Get(h.base + "/" + testBucket + "/missing.jpg")
	if err != nil {
		t.Fatal(err)
	}
	expectError(t, resp, http.StatusNotFound, "NoSuchKey")
}

func TestPublicReadSupportsRange(t *testing.T) {
	h := publicHarness(t)
	put(t, h.client, "a.txt", "0123456789")

	req, err := http.NewRequest(http.MethodGet, h.base+"/"+testBucket+"/a.txt", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Range", "bytes=2-4")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusPartialContent || string(raw) != "234" {
		t.Errorf("range request: status %d, body %q", resp.StatusCode, raw)
	}
}
