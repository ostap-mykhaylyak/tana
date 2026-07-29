// Package s3client is the agent's side of the wire: a small, signing
// S3 client with no dependencies.
//
// It exists rather than an SDK because the agent needs five calls, and
// an SDK brings a hundred modules, a retry framework, a credential
// chain and a plugin system to provide them. The signing code here is
// written from the AWS specification, deliberately not shared with the
// server's verifier: an implementation that signs and verifies with
// the same function proves only that the function is deterministic.
package s3client

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ostap-mykhaylyak/tana/internal/config"
)

const (
	algorithm  = "AWS4-HMAC-SHA256"
	service    = "s3"
	terminator = "aws4_request"
	amzDate    = "20060102T150405Z"
	shortDate  = "20060102"

	unsignedPayload = "UNSIGNED-PAYLOAD"
)

// Client talks to one bucket on one endpoint.
type Client struct {
	endpoint  *url.URL
	bucket    string
	region    string
	accessKey string
	secretKey string
	http      *http.Client
}

// Object is one entry of a listing.
type Object struct {
	Key          string
	Size         int64
	ETag         string
	LastModified time.Time
}

// Info describes an object's metadata.
type Info struct {
	Size         int64
	ETag         string
	LastModified time.Time
}

// ErrNotFound reports that a key is absent.
type ErrNotFound struct{ Key string }

func (e ErrNotFound) Error() string { return "no such key: " + e.Key }

// Error is a non-2xx response from the store.
type Error struct {
	Status  int
	Code    string
	Message string
}

func (e *Error) Error() string {
	if e.Code == "" {
		return fmt.Sprintf("s3: http %d", e.Status)
	}
	return fmt.Sprintf("s3: %s: %s", e.Code, e.Message)
}

// Retryable reports whether repeating the request could help. A 4xx
// other than 408 means the request itself is wrong, and retrying a
// wrong request only wastes a queue slot.
func (e *Error) Retryable() bool {
	return e.Status >= 500 || e.Status == http.StatusRequestTimeout || e.Status == http.StatusTooManyRequests
}

// New builds a client for a site's backend.
func New(b config.Backend, timeout time.Duration) (*Client, error) {
	u, err := url.Parse(b.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("s3 client: endpoint: %w", err)
	}
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}
	return &Client{
		endpoint:  u,
		bucket:    b.Bucket,
		region:    b.Region,
		accessKey: b.AccessKey,
		secretKey: b.SecretKey,
		// The timeout has to cover a whole object, not a round trip:
		// a large downloadable product on a slow link is a legitimate,
		// slow request, not a stuck one.
		http: &http.Client{Timeout: timeout},
	}, nil
}

// Bucket returns the bucket this client is bound to.
func (c *Client) Bucket() string { return c.bucket }

// Endpoint returns the store URL.
func (c *Client) Endpoint() string { return c.endpoint.String() }

// PutFile uploads a local file.
//
// The file is hashed before it is sent so the request can declare its
// content hash, which the store checks on arrival. That makes a
// corrupted transfer an error rather than a silently wrong object, and
// costs one extra read of a file that is in the page cache anyway.
func (c *Client) PutFile(ctx context.Context, key, path string) (string, error) {
	hash, size, err := hashFile(path)
	if err != nil {
		return "", err
	}
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	resp, err := c.do(ctx, request{
		method:  http.MethodPut,
		key:     key,
		body:    f,
		length:  size,
		payload: hash,
	})
	if err != nil {
		return "", err
	}
	defer drain(resp)
	return strings.Trim(resp.Header.Get("ETag"), `"`), nil
}

// Put uploads bytes already in memory.
func (c *Client) Put(ctx context.Context, key string, body []byte) (string, error) {
	sum := sha256.Sum256(body)
	resp, err := c.do(ctx, request{
		method:  http.MethodPut,
		key:     key,
		body:    strings.NewReader(string(body)),
		length:  int64(len(body)),
		payload: hex.EncodeToString(sum[:]),
	})
	if err != nil {
		return "", err
	}
	defer drain(resp)
	return strings.Trim(resp.Header.Get("ETag"), `"`), nil
}

// Get opens an object for reading. The caller closes the reader.
func (c *Client) Get(ctx context.Context, key string) (io.ReadCloser, Info, error) {
	resp, err := c.do(ctx, request{method: http.MethodGet, key: key})
	if err != nil {
		return nil, Info{}, err
	}
	return resp.Body, infoOf(resp), nil
}

// GetRange opens a byte range of an object.
func (c *Client) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	resp, err := c.do(ctx, request{
		method: http.MethodGet,
		key:    key,
		header: http.Header{"Range": {fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)}},
	})
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

// Head returns an object's metadata.
func (c *Client) Head(ctx context.Context, key string) (Info, error) {
	resp, err := c.do(ctx, request{method: http.MethodHead, key: key})
	if err != nil {
		return Info{}, err
	}
	defer drain(resp)
	return infoOf(resp), nil
}

// Delete removes an object. Deleting what is not there succeeds.
func (c *Client) Delete(ctx context.Context, key string) error {
	resp, err := c.do(ctx, request{method: http.MethodDelete, key: key})
	if err != nil {
		var notFound ErrNotFound
		if ok := asNotFound(err, &notFound); ok {
			return nil
		}
		return err
	}
	drain(resp)
	return nil
}

// List calls fn for every object under prefix, following pagination.
func (c *Client) List(ctx context.Context, prefix string, fn func(Object) error) error {
	token := ""
	for {
		q := url.Values{"list-type": {"2"}}
		if prefix != "" {
			q.Set("prefix", prefix)
		}
		if token != "" {
			q.Set("continuation-token", token)
		}
		resp, err := c.do(ctx, request{method: http.MethodGet, query: q})
		if err != nil {
			return err
		}
		raw, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
		resp.Body.Close()
		if err != nil {
			return err
		}

		var res struct {
			IsTruncated           bool   `xml:"IsTruncated"`
			NextContinuationToken string `xml:"NextContinuationToken"`
			Contents              []struct {
				Key          string    `xml:"Key"`
				Size         int64     `xml:"Size"`
				ETag         string    `xml:"ETag"`
				LastModified time.Time `xml:"LastModified"`
			} `xml:"Contents"`
		}
		if err := xml.Unmarshal(raw, &res); err != nil {
			return fmt.Errorf("s3 client: malformed listing: %w", err)
		}
		for _, o := range res.Contents {
			if err := fn(Object{
				Key: o.Key, Size: o.Size,
				ETag:         strings.Trim(o.ETag, `"`),
				LastModified: o.LastModified,
			}); err != nil {
				return err
			}
		}
		if !res.IsTruncated || res.NextContinuationToken == "" {
			return nil
		}
		token = res.NextContinuationToken
	}
}

// Presign builds a time-limited URL for an object, the form used to
// hand a browser a download without handing it credentials.
func (c *Client) Presign(method, key string, expires time.Duration) string {
	now := time.Now().UTC()
	scope := now.Format(shortDate) + "/" + c.region + "/" + service + "/" + terminator

	q := url.Values{}
	q.Set("X-Amz-Algorithm", algorithm)
	q.Set("X-Amz-Credential", c.accessKey+"/"+scope)
	q.Set("X-Amz-Date", now.Format(amzDate))
	q.Set("X-Amz-Expires", strconv.Itoa(int(expires.Seconds())))
	q.Set("X-Amz-SignedHeaders", "host")

	path := c.path(key)
	canonical := strings.Join([]string{
		method,
		encodePath(path),
		encodeQuery(q),
		"host:" + c.endpoint.Host + "\n",
		"host",
		unsignedPayload,
	}, "\n")
	q.Set("X-Amz-Signature", c.sign(now, scope, canonical))

	return c.endpoint.String() + path + "?" + q.Encode()
}

// request is one call to build and sign.
type request struct {
	method  string
	key     string
	query   url.Values
	body    io.Reader
	length  int64
	payload string
	header  http.Header
}

// do signs and performs a request, turning a non-2xx into an error.
func (c *Client) do(ctx context.Context, req request) (*http.Response, error) {
	path := c.path(req.key)
	u := c.endpoint.String() + path
	if len(req.query) > 0 {
		u += "?" + req.query.Encode()
	}

	r, err := http.NewRequestWithContext(ctx, req.method, u, req.body)
	if err != nil {
		return nil, err
	}
	for k, vals := range req.header {
		for _, v := range vals {
			r.Header.Add(k, v)
		}
	}
	if req.length > 0 {
		r.ContentLength = req.length
	}
	payload := req.payload
	if payload == "" {
		payload = emptyHash
	}
	c.signRequest(r, payload)

	resp, err := c.http.Do(r)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return resp, nil
	}
	defer drain(resp)
	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound{Key: req.key}
	}
	return nil, decodeError(resp)
}

// path is the request path for a key, path-style.
func (c *Client) path(key string) string {
	if key == "" {
		return "/" + c.bucket
	}
	return "/" + c.bucket + "/" + key
}

// signRequest adds the sigv4 Authorization header.
func (c *Client) signRequest(r *http.Request, payload string) {
	now := time.Now().UTC()
	stamp := now.Format(amzDate)
	scope := now.Format(shortDate) + "/" + c.region + "/" + service + "/" + terminator

	r.Header.Set("X-Amz-Date", stamp)
	r.Header.Set("X-Amz-Content-Sha256", payload)
	if r.Host == "" {
		r.Host = c.endpoint.Host
	}

	signed := []string{"host", "x-amz-content-sha256", "x-amz-date"}
	sort.Strings(signed)

	var headers strings.Builder
	for _, name := range signed {
		value := r.Header.Get(name)
		if name == "host" {
			value = r.Host
		}
		headers.WriteString(name + ":" + strings.Join(strings.Fields(value), " ") + "\n")
	}

	canonical := strings.Join([]string{
		r.Method,
		encodePath(r.URL.Path),
		encodeQuery(r.URL.Query()),
		headers.String(),
		strings.Join(signed, ";"),
		payload,
	}, "\n")

	r.Header.Set("Authorization", fmt.Sprintf(
		"%s Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		algorithm, c.accessKey, scope, strings.Join(signed, ";"),
		c.sign(now, scope, canonical)))
}

// sign computes a signature over a canonical request.
func (c *Client) sign(now time.Time, scope, canonical string) string {
	sum := sha256.Sum256([]byte(canonical))
	sts := strings.Join([]string{
		algorithm, now.Format(amzDate), scope, hex.EncodeToString(sum[:]),
	}, "\n")

	k := mac([]byte("AWS4"+c.secretKey), []byte(now.Format(shortDate)))
	k = mac(k, []byte(c.region))
	k = mac(k, []byte(service))
	k = mac(k, []byte(terminator))
	return hex.EncodeToString(mac(k, []byte(sts)))
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

// rfc3986 percent-encodes everything outside the unreserved set. Not
// url.QueryEscape: that encodes a space as '+' and leaves '~' alone,
// and either difference breaks every signature.
func rfc3986(s string) string {
	const keep = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.~"
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		if strings.IndexByte(keep, s[i]) >= 0 {
			b.WriteByte(s[i])
			continue
		}
		fmt.Fprintf(&b, "%%%02X", s[i])
	}
	return b.String()
}

// hashFile returns a file's hex sha256 and size.
func hashFile(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

// infoOf reads object metadata from a response.
func infoOf(resp *http.Response) Info {
	i := Info{
		Size: resp.ContentLength,
		ETag: strings.Trim(resp.Header.Get("ETag"), `"`),
	}
	if t, err := http.ParseTime(resp.Header.Get("Last-Modified")); err == nil {
		i.LastModified = t
	}
	return i
}

// decodeError turns an S3 error document into an Error.
func decodeError(resp *http.Response) error {
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	e := &Error{Status: resp.StatusCode}
	var doc struct {
		Code    string `xml:"Code"`
		Message string `xml:"Message"`
	}
	if err := xml.Unmarshal(raw, &doc); err == nil {
		e.Code, e.Message = doc.Code, doc.Message
	}
	return e
}

// asNotFound reports whether err is an ErrNotFound.
func asNotFound(err error, target *ErrNotFound) bool {
	nf, ok := err.(ErrNotFound)
	if ok {
		*target = nf
	}
	return ok
}

// drain consumes and closes a response body so the connection can be
// reused rather than torn down after every call.
func drain(resp *http.Response) {
	io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	resp.Body.Close()
}

// emptyHash is the sha256 of no bytes.
var emptyHash = func() string {
	sum := sha256.Sum256(nil)
	return hex.EncodeToString(sum[:])
}()
