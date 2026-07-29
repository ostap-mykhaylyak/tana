package s3

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ostap-mykhaylyak/tana/internal/config"
	"github.com/ostap-mykhaylyak/tana/internal/index"
	"github.com/ostap-mykhaylyak/tana/internal/store"
)

// A minimal sigv4 client, written from the AWS specification rather
// than by calling the server's own signing code. Verifying a signature
// with the function that produced it proves only that the function is
// deterministic; the point of these tests is that an independent
// implementation of the documented algorithm interoperates.

const (
	testRegion = "tana"
	testBucket = "shop-uploads"
	testAK     = "TANATESTACCESSKEY000"
	testSK     = "testsecretkey0123456789abcdefghij"
)

type client struct {
	t    *testing.T
	base string
	ak   string
	sk   string
}

// harness is a running S3 server over a real store.
type harness struct {
	*client
	srv   *httptest.Server
	store *store.Store
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	dir := t.TempDir()
	idx, err := index.Open(filepath.Join(dir, "index.db"))
	if err != nil {
		t.Fatal(err)
	}
	conf := config.Store{
		Data:   filepath.Join(dir, "data"),
		Region: testRegion,
		Buckets: []config.Bucket{
			{Name: testBucket, AccessKey: testAK, SecretKey: testSK},
			{Name: "other-site", AccessKey: "OTHERKEY000000000000", SecretKey: "othersecret"},
		},
		GC: config.GC{Interval: config.Duration(time.Hour), Grace: config.Duration(72 * time.Hour)},
	}
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	st, err := store.New(conf, idx, discard, discard)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(New(st, testRegion, discard, discard))
	t.Cleanup(func() {
		srv.Close()
		st.Close()
		idx.Close()
	})
	return &harness{
		client: &client{t: t, base: srv.URL, ak: testAK, sk: testSK},
		srv:    srv,
		store:  st,
	}
}

// as returns a client using different credentials.
func (h *harness) as(ak, sk string) *client {
	return &client{t: h.t, base: h.base, ak: ak, sk: sk}
}

// request is one signed call.
type request struct {
	method string
	path   string
	query  url.Values
	body   []byte
	header http.Header
	// unsigned sends UNSIGNED-PAYLOAD instead of the body hash.
	unsigned bool
	// skew shifts the request clock, to exercise the skew window.
	skew time.Duration
	// tamper corrupts the signature after it is computed.
	tamper bool
}

// do signs and sends a request.
func (c *client) do(req request) *http.Response {
	c.t.Helper()

	u := c.base + req.path
	if len(req.query) > 0 {
		u += "?" + req.query.Encode()
	}
	r, err := http.NewRequest(req.method, u, bytes.NewReader(req.body))
	if err != nil {
		c.t.Fatal(err)
	}
	for k, vals := range req.header {
		for _, v := range vals {
			r.Header.Add(k, v)
		}
	}

	payload := unsignedPayloadLiteral
	if !req.unsigned {
		sum := sha256.Sum256(req.body)
		payload = hex.EncodeToString(sum[:])
	}
	now := time.Now().UTC().Add(req.skew)
	c.sign(r, payload, now, req.query)
	if req.tamper {
		auth := r.Header.Get("Authorization")
		r.Header.Set("Authorization", auth[:len(auth)-4]+"dead")
	}

	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		c.t.Fatal(err)
	}
	return resp
}

const unsignedPayloadLiteral = "UNSIGNED-PAYLOAD"

// sign adds an Authorization header per the sigv4 specification.
func (c *client) sign(r *http.Request, payloadHash string, now time.Time, query url.Values) {
	amzDate := now.Format("20060102T150405Z")
	scope := now.Format("20060102") + "/" + testRegion + "/s3/aws4_request"

	r.Header.Set("X-Amz-Date", amzDate)
	r.Header.Set("X-Amz-Content-Sha256", payloadHash)

	signed := []string{"host", "x-amz-content-sha256", "x-amz-date"}
	if r.Header.Get("Content-Md5") != "" {
		signed = append(signed, "content-md5")
	}
	sort.Strings(signed)

	var headers strings.Builder
	for _, name := range signed {
		var value string
		switch name {
		case "host":
			value = r.Host
			if value == "" {
				value = r.URL.Host
			}
		default:
			value = strings.Join(strings.Fields(r.Header.Get(name)), " ")
		}
		headers.WriteString(name + ":" + value + "\n")
	}

	canonical := strings.Join([]string{
		r.Method,
		encodePath(r.URL.Path),
		encodeQuery(query),
		headers.String(),
		strings.Join(signed, ";"),
		payloadHash,
	}, "\n")

	sum := sha256.Sum256([]byte(canonical))
	sts := strings.Join([]string{
		"AWS4-HMAC-SHA256", amzDate, scope, hex.EncodeToString(sum[:]),
	}, "\n")

	sig := hex.EncodeToString(mac(deriveKey(c.sk, now), []byte(sts)))
	r.Header.Set("Authorization", fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		c.ak, scope, strings.Join(signed, ";"), sig))
}

// presign builds a time-limited URL, the form used for download links.
func (c *client) presign(method, path string, expires time.Duration) string {
	return c.presignAt(method, path, expires, time.Now().UTC())
}

// presignAt builds a presigned URL as if it had been issued at a given
// moment, so a test can produce a link that is genuinely expired
// rather than one that is merely malformed.
func (c *client) presignAt(method, path string, expires time.Duration, now time.Time) string {
	c.t.Helper()
	amzDate := now.Format("20060102T150405Z")
	scope := now.Format("20060102") + "/" + testRegion + "/s3/aws4_request"

	base, err := url.Parse(c.base)
	if err != nil {
		c.t.Fatal(err)
	}
	q := url.Values{}
	q.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	q.Set("X-Amz-Credential", c.ak+"/"+scope)
	q.Set("X-Amz-Date", amzDate)
	q.Set("X-Amz-Expires", fmt.Sprint(int(expires.Seconds())))
	q.Set("X-Amz-SignedHeaders", "host")

	canonical := strings.Join([]string{
		method,
		encodePath(path),
		encodeQuery(q),
		"host:" + base.Host + "\n",
		"host",
		unsignedPayloadLiteral,
	}, "\n")
	sum := sha256.Sum256([]byte(canonical))
	sts := strings.Join([]string{"AWS4-HMAC-SHA256", amzDate, scope, hex.EncodeToString(sum[:])}, "\n")
	q.Set("X-Amz-Signature", hex.EncodeToString(mac(deriveKey(c.sk, now), []byte(sts))))

	return c.base + path + "?" + q.Encode()
}

// deriveKey builds the scoped signing key.
func deriveKey(secret string, now time.Time) []byte {
	k := mac([]byte("AWS4"+secret), []byte(now.Format("20060102")))
	k = mac(k, []byte(testRegion))
	k = mac(k, []byte("s3"))
	return mac(k, []byte("aws4_request"))
}

func mac(key, data []byte) []byte {
	m := hmac.New(sha256.New, key)
	m.Write(data)
	return m.Sum(nil)
}

// encodePath percent-encodes a path, keeping separators.
func encodePath(p string) string {
	if p == "" {
		return "/"
	}
	parts := strings.Split(p, "/")
	for i, s := range parts {
		parts[i] = rfc3986(s)
	}
	return strings.Join(parts, "/")
}

// encodeQuery renders the canonical query string.
func encodeQuery(q url.Values) string {
	keys := make([]string, 0, len(q))
	for k := range q {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		vals := append([]string(nil), q[k]...)
		sort.Strings(vals)
		for _, v := range vals {
			if b.Len() > 0 {
				b.WriteByte('&')
			}
			b.WriteString(rfc3986(k) + "=" + rfc3986(v))
		}
	}
	return b.String()
}

// rfc3986 percent-encodes everything outside the unreserved set.
func rfc3986(s string) string {
	const keep = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.~"
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if strings.IndexByte(keep, s[i]) >= 0 {
			b.WriteByte(s[i])
			continue
		}
		fmt.Fprintf(&b, "%%%02X", s[i])
	}
	return b.String()
}

// --- assertions -------------------------------------------------------

func readBody(t *testing.T, resp *http.Response) []byte {
	t.Helper()
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func expectStatus(t *testing.T, resp *http.Response, want int) []byte {
	t.Helper()
	raw := readBody(t, resp)
	if resp.StatusCode != want {
		t.Fatalf("status = %d, want %d\nbody: %s", resp.StatusCode, want, raw)
	}
	return raw
}

// expectError asserts an S3 error document with the given code.
func expectError(t *testing.T, resp *http.Response, wantStatus int, wantCode string) {
	t.Helper()
	raw := readBody(t, resp)
	if resp.StatusCode != wantStatus {
		t.Fatalf("status = %d, want %d\nbody: %s", resp.StatusCode, wantStatus, raw)
	}
	var e APIError
	if err := xml.Unmarshal(raw, &e); err != nil {
		t.Fatalf("response is not an S3 error document: %s", raw)
	}
	if e.Code != wantCode {
		t.Errorf("error code = %q, want %q", e.Code, wantCode)
	}
}

func decodeXML(t *testing.T, raw []byte, v any) {
	t.Helper()
	if err := xml.Unmarshal(raw, v); err != nil {
		t.Fatalf("decode: %v\nbody: %s", err, raw)
	}
}

// hashOf is the hex sha256 of a string, for tests that declare a
// content hash by hand.
func hashOf(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// storeConfigWith rebuilds the test store configuration around one
// modified bucket, for policy tests.
func storeConfigWith(b config.Bucket) config.Store {
	return config.Store{
		Region: testRegion,
		Buckets: []config.Bucket{
			b,
			{Name: "other-site", AccessKey: "OTHERKEY000000000000", SecretKey: "othersecret"},
		},
		GC: config.GC{Interval: config.Duration(time.Hour), Grace: config.Duration(72 * time.Hour)},
	}
}
