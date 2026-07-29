package s3

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// AWS Signature Version 4, the subset S3 uses.
//
// The signature covers the method, the path, the query, a chosen set
// of headers and a hash of the body, so nothing a client committed to
// can be altered in flight. tana verifies it rather than trusting the
// network, because "the store is only on the LAN" is a statement about
// today's network diagram and not about tomorrow's.
const (
	algorithm     = "AWS4-HMAC-SHA256"
	service       = "s3"
	terminator    = "aws4_request"
	amzDateFormat = "20060102T150405Z"
	dateFormat    = "20060102"

	// unsignedPayload is what a client sends when it will not hash the
	// body — always the case for presigned URLs, where the body is not
	// known when the URL is made.
	unsignedPayload = "UNSIGNED-PAYLOAD"
	// streamingSigned frames the body in signed chunks.
	streamingSigned = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
	// streamingSignedTrailer is the same with trailing headers.
	streamingSignedTrailer = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"
	// streamingUnsignedTrailer frames the body but signs no chunk.
	streamingUnsignedTrailer = "STREAMING-UNSIGNED-PAYLOAD-TRAILER"

	// maxSkew is how far a request's clock may differ from ours. AWS
	// uses fifteen minutes, and matching it means a client that works
	// against S3 works here.
	maxSkew = 15 * time.Minute
)

// credentials are one bucket's key pair.
type credentials struct {
	AccessKey string
	SecretKey string
	Bucket    string
}

// keyLookup resolves an access key to its credentials.
type keyLookup func(accessKey string) (credentials, bool)

// signature is everything verification extracted from a request, and
// what the chunked reader needs to continue the signature chain.
type signature struct {
	creds     credentials
	scope     string
	date      time.Time
	seed      string // the request signature, seed of the chunk chain
	payload   string // the x-amz-content-sha256 value, verbatim
	presigned bool
}

// streaming reports whether the body is aws-chunked framed.
func (s signature) streaming() bool {
	switch s.payload {
	case streamingSigned, streamingSignedTrailer, streamingUnsignedTrailer:
		return true
	}
	return false
}

// chunksSigned reports whether each chunk carries its own signature.
func (s signature) chunksSigned() bool {
	return s.payload == streamingSigned || s.payload == streamingSignedTrailer
}

// verifier checks request signatures.
type verifier struct {
	lookup keyLookup
	region string
	now    func() time.Time
}

// verify authenticates a request, returning what was proven about it.
func (v *verifier) verify(r *http.Request) (signature, *APIError) {
	if r.URL.Query().Get("X-Amz-Signature") != "" {
		return v.verifyPresigned(r)
	}
	return v.verifyHeader(r)
}

// verifyHeader checks an Authorization header signature.
func (v *verifier) verifyHeader(r *http.Request) (signature, *APIError) {
	auth := r.Header.Get("Authorization")
	if auth == "" {
		return signature{}, errMissingAuth()
	}
	rest, ok := strings.CutPrefix(auth, algorithm+" ")
	if !ok {
		return signature{}, errInvalidRequest("unsupported authorization algorithm")
	}

	var credential, signedHeaders, provided string
	for _, part := range strings.Split(rest, ",") {
		k, val, found := strings.Cut(strings.TrimSpace(part), "=")
		if !found {
			continue
		}
		switch k {
		case "Credential":
			credential = val
		case "SignedHeaders":
			signedHeaders = val
		case "Signature":
			provided = val
		}
	}
	if credential == "" || signedHeaders == "" || provided == "" {
		return signature{}, errInvalidRequest("malformed authorization header")
	}

	creds, scope, apiErr := v.resolve(credential)
	if apiErr != nil {
		return signature{}, apiErr
	}
	when, apiErr := v.requestTime(r.Header.Get("X-Amz-Date"), r.Header.Get("Date"))
	if apiErr != nil {
		return signature{}, apiErr
	}

	payload := r.Header.Get("X-Amz-Content-Sha256")
	if payload == "" {
		// S3 requires the header for sigv4; without it there is nothing
		// binding the body to the signature.
		return signature{}, errInvalidRequest("missing X-Amz-Content-Sha256")
	}

	canonical := canonicalRequest(r, strings.Split(signedHeaders, ";"), payload, nil)
	sts := stringToSign(when, scope, canonical)
	expected := sign(creds.SecretKey, when, v.region, sts)
	if !hmac.Equal([]byte(expected), []byte(provided)) {
		return signature{}, errSignatureMismatch()
	}
	return signature{
		creds: creds, scope: scope, date: when, seed: expected, payload: payload,
	}, nil
}

// verifyPresigned checks a signature carried in the query string, the
// form used for time-limited download links.
func (v *verifier) verifyPresigned(r *http.Request) (signature, *APIError) {
	q := r.URL.Query()
	if q.Get("X-Amz-Algorithm") != algorithm {
		return signature{}, errInvalidRequest("unsupported presigned algorithm")
	}
	credential := q.Get("X-Amz-Credential")
	signedHeaders := q.Get("X-Amz-SignedHeaders")
	provided := q.Get("X-Amz-Signature")
	if credential == "" || signedHeaders == "" {
		return signature{}, errInvalidRequest("malformed presigned request")
	}

	creds, scope, apiErr := v.resolve(credential)
	if apiErr != nil {
		return signature{}, apiErr
	}
	when, err := time.Parse(amzDateFormat, q.Get("X-Amz-Date"))
	if err != nil {
		return signature{}, errInvalidRequest("malformed X-Amz-Date")
	}
	expires, err := strconv.Atoi(q.Get("X-Amz-Expires"))
	if err != nil || expires <= 0 {
		return signature{}, errInvalidRequest("malformed X-Amz-Expires")
	}
	// A presigned URL carries its own lifetime, so the skew window does
	// not apply: only the deadline does.
	if v.now().After(when.Add(time.Duration(expires) * time.Second)) {
		return signature{}, errRequestExpired()
	}

	// The signature cannot cover itself.
	filtered := make(url.Values, len(q))
	for k, vals := range q {
		if k == "X-Amz-Signature" {
			continue
		}
		filtered[k] = vals
	}

	canonical := canonicalRequest(r, strings.Split(signedHeaders, ";"), unsignedPayload, filtered)
	sts := stringToSign(when, scope, canonical)
	expected := sign(creds.SecretKey, when, v.region, sts)
	if !hmac.Equal([]byte(expected), []byte(provided)) {
		return signature{}, errSignatureMismatch()
	}
	return signature{
		creds: creds, scope: scope, date: when, seed: expected,
		payload: unsignedPayload, presigned: true,
	}, nil
}

// resolve splits a credential scope and looks up the key.
func (v *verifier) resolve(credential string) (credentials, string, *APIError) {
	parts := strings.Split(credential, "/")
	if len(parts) != 5 {
		return credentials{}, "", errInvalidRequest("malformed credential scope")
	}
	accessKey := parts[0]
	scope := strings.Join(parts[1:], "/")
	if parts[2] != v.region {
		return credentials{}, "", errInvalidRequest(
			fmt.Sprintf("credential scope region %q does not match the store region %q", parts[2], v.region))
	}
	if parts[3] != service || parts[4] != terminator {
		return credentials{}, "", errInvalidRequest("malformed credential scope")
	}
	creds, ok := v.lookup(accessKey)
	if !ok {
		return credentials{}, "", errInvalidAccessKeyID()
	}
	return creds, scope, nil
}

// requestTime parses and range-checks the request clock.
func (v *verifier) requestTime(amzDate, httpDate string) (time.Time, *APIError) {
	var when time.Time
	var err error
	switch {
	case amzDate != "":
		when, err = time.Parse(amzDateFormat, amzDate)
	case httpDate != "":
		when, err = http.ParseTime(httpDate)
	default:
		return time.Time{}, errInvalidRequest("missing X-Amz-Date")
	}
	if err != nil {
		return time.Time{}, errInvalidRequest("malformed request date")
	}
	if d := v.now().Sub(when); d > maxSkew || d < -maxSkew {
		return time.Time{}, errRequestExpired()
	}
	return when, nil
}

// canonicalRequest builds the document the signature is computed over.
// query overrides r.URL's when non-nil, which presigned requests need
// so the signature does not cover itself.
func canonicalRequest(r *http.Request, signedHeaders []string, payloadHash string, query url.Values) string {
	if query == nil {
		query = r.URL.Query()
	}
	sort.Strings(signedHeaders)

	var headers strings.Builder
	for _, name := range signedHeaders {
		headers.WriteString(name)
		headers.WriteByte(':')
		headers.WriteString(canonicalHeaderValue(r, name))
		headers.WriteByte('\n')
	}

	return strings.Join([]string{
		r.Method,
		canonicalURI(r.URL),
		canonicalQuery(query),
		headers.String(),
		strings.Join(signedHeaders, ";"),
		payloadHash,
	}, "\n")
}

// canonicalHeaderValue returns a header as the signature saw it.
// The Host header is special: Go moves it off the header map.
func canonicalHeaderValue(r *http.Request, name string) string {
	if name == "host" {
		if r.Host != "" {
			return r.Host
		}
		return r.URL.Host
	}
	vals := r.Header.Values(http.CanonicalHeaderKey(name))
	trimmed := make([]string, 0, len(vals))
	for _, v := range vals {
		trimmed = append(trimmed, strings.Join(strings.Fields(v), " "))
	}
	return strings.Join(trimmed, ",")
}

// canonicalURI is the request path, encoded but with separators kept.
func canonicalURI(u *url.URL) string {
	path := u.EscapedPath()
	if path == "" {
		return "/"
	}
	// EscapedPath leaves some characters raw that the signature expects
	// encoded, so re-encode segment by segment from the decoded form.
	segments := strings.Split(u.Path, "/")
	for i, s := range segments {
		segments[i] = uriEncode(s)
	}
	return strings.Join(segments, "/")
}

// canonicalQuery sorts and encodes the query string.
func canonicalQuery(q url.Values) string {
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
			b.WriteString(uriEncode(k))
			b.WriteByte('=')
			b.WriteString(uriEncode(v))
		}
	}
	return b.String()
}

// uriEncode percent-encodes per RFC 3986, which is not what
// url.QueryEscape does: it encodes a space as '+' and leaves '~'
// alone, and either difference breaks every signature.
func uriEncode(s string) string {
	const unreserved = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.~"
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if strings.IndexByte(unreserved, c) >= 0 {
			b.WriteByte(c)
			continue
		}
		fmt.Fprintf(&b, "%%%02X", c)
	}
	return b.String()
}

// stringToSign wraps the canonical request with its scope.
func stringToSign(when time.Time, scope, canonical string) string {
	sum := sha256.Sum256([]byte(canonical))
	return strings.Join([]string{
		algorithm,
		when.UTC().Format(amzDateFormat),
		scope,
		hex.EncodeToString(sum[:]),
	}, "\n")
}

// sign derives the request signature.
func sign(secret string, when time.Time, region, stringToSign string) string {
	return hex.EncodeToString(hmacSHA256(signingKey(secret, when, region), []byte(stringToSign)))
}

// signingKey derives the date/region/service scoped key. Each step
// narrows what the key can be replayed against: a key stolen today
// cannot sign tomorrow's requests, nor another region's.
func signingKey(secret string, when time.Time, region string) []byte {
	k := hmacSHA256([]byte("AWS4"+secret), []byte(when.UTC().Format(dateFormat)))
	k = hmacSHA256(k, []byte(region))
	k = hmacSHA256(k, []byte(service))
	return hmacSHA256(k, []byte(terminator))
}

func hmacSHA256(key, data []byte) []byte {
	m := hmac.New(sha256.New, key)
	m.Write(data)
	return m.Sum(nil)
}

// emptyPayloadHash is sha256 of no bytes, which the signature uses for
// requests without a body.
var emptyPayloadHash = func() string {
	sum := sha256.Sum256(nil)
	return hex.EncodeToString(sum[:])
}()
