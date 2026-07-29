package s3

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

// putChunked uploads a body using aws-chunked framing with a real
// signature chain, which is what `aws s3 cp` sends when it streams.
//
// The chain is built here from the specification, so the server's
// verification is checked against an independent producer rather than
// against itself.
func (c *client) putChunked(key string, chunks []string, signChunks bool) *http.Response {
	c.t.Helper()

	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	scope := now.Format("20060102") + "/" + testRegion + "/s3/aws4_request"
	payload := streamingSigned
	if !signChunks {
		payload = streamingUnsignedTrailer
	}

	var decoded int64
	for _, ch := range chunks {
		decoded += int64(len(ch))
	}

	path := "/" + testBucket + "/" + key
	r, err := http.NewRequest(http.MethodPut, c.base+path, nil)
	if err != nil {
		c.t.Fatal(err)
	}
	r.Header.Set("X-Amz-Date", amzDate)
	r.Header.Set("X-Amz-Content-Sha256", payload)
	r.Header.Set("X-Amz-Decoded-Content-Length", fmt.Sprint(decoded))

	signed := []string{"host", "x-amz-content-sha256", "x-amz-date", "x-amz-decoded-content-length"}
	headers := strings.Builder{}
	for _, name := range signed {
		value := r.Header.Get(name)
		if name == "host" {
			value = r.URL.Host
		}
		headers.WriteString(name + ":" + value + "\n")
	}
	canonical := strings.Join([]string{
		http.MethodPut, encodePath(path), "", headers.String(),
		strings.Join(signed, ";"), payload,
	}, "\n")
	sum := sha256.Sum256([]byte(canonical))
	sts := strings.Join([]string{"AWS4-HMAC-SHA256", amzDate, scope, hex.EncodeToString(sum[:])}, "\n")

	key4 := deriveKey(c.sk, now)
	seed := hex.EncodeToString(mac(key4, []byte(sts)))
	r.Header.Set("Authorization", fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		c.ak, scope, strings.Join(signed, ";"), seed))

	// Frame the body, chaining each chunk signature from the previous.
	var body strings.Builder
	prev := seed
	emit := func(data string) {
		if signChunks {
			h := sha256.Sum256([]byte(data))
			chunkSTS := strings.Join([]string{
				chunkAlgorithm, amzDate, scope, prev,
				emptyPayloadHash, hex.EncodeToString(h[:]),
			}, "\n")
			prev = hex.EncodeToString(mac(key4, []byte(chunkSTS)))
			fmt.Fprintf(&body, "%x;chunk-signature=%s\r\n%s\r\n", len(data), prev, data)
			return
		}
		fmt.Fprintf(&body, "%x\r\n%s\r\n", len(data), data)
	}
	for _, ch := range chunks {
		emit(ch)
	}
	emit("") // the terminating zero-length chunk
	body.WriteString("\r\n")

	r.Body = http.NoBody
	req, err := http.NewRequest(http.MethodPut, c.base+path, strings.NewReader(body.String()))
	if err != nil {
		c.t.Fatal(err)
	}
	req.Header = r.Header

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	return resp
}

func TestChunkedUploadSigned(t *testing.T) {
	h := newHarness(t)
	chunks := []string{strings.Repeat("a", 100), strings.Repeat("b", 50), "tail"}

	resp := h.putChunked("streamed.bin", chunks, true)
	expectStatus(t, resp, http.StatusOK)

	resp = h.do(request{method: http.MethodGet, path: "/" + testBucket + "/streamed.bin"})
	got := string(expectStatus(t, resp, http.StatusOK))
	if want := strings.Join(chunks, ""); got != want {
		t.Errorf("reassembled %d bytes, want %d", len(got), len(want))
	}
}

func TestChunkedUploadUnsigned(t *testing.T) {
	h := newHarness(t)
	// The trailer variant frames the body but signs no chunk. It is
	// still an authenticated request; only the body is uncovered.
	chunks := []string{"hello ", "world"}

	resp := h.putChunked("unsigned.bin", chunks, false)
	expectStatus(t, resp, http.StatusOK)

	resp = h.do(request{method: http.MethodGet, path: "/" + testBucket + "/unsigned.bin"})
	if got := string(expectStatus(t, resp, http.StatusOK)); got != "hello world" {
		t.Errorf("reassembled %q", got)
	}
}

func TestChunkedUploadRejectsBrokenChain(t *testing.T) {
	h := newHarness(t)
	// A chunk altered in flight must break the chain. Accepting the
	// framing without verifying it would turn a signed upload into an
	// unsigned body behind a signed header, which is the failure this
	// whole mechanism exists to prevent.
	c := h.client
	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	scope := now.Format("20060102") + "/" + testRegion + "/s3/aws4_request"

	path := "/" + testBucket + "/tampered.bin"
	req, err := http.NewRequest(http.MethodPut, c.base+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", streamingSigned)
	req.Header.Set("X-Amz-Decoded-Content-Length", "5")

	signed := []string{"host", "x-amz-content-sha256", "x-amz-date", "x-amz-decoded-content-length"}
	var headers strings.Builder
	for _, name := range signed {
		value := req.Header.Get(name)
		if name == "host" {
			value = req.URL.Host
		}
		headers.WriteString(name + ":" + value + "\n")
	}
	canonical := strings.Join([]string{
		http.MethodPut, encodePath(path), "", headers.String(),
		strings.Join(signed, ";"), streamingSigned,
	}, "\n")
	sum := sha256.Sum256([]byte(canonical))
	sts := strings.Join([]string{"AWS4-HMAC-SHA256", amzDate, scope, hex.EncodeToString(sum[:])}, "\n")
	seed := hex.EncodeToString(mac(deriveKey(c.sk, now), []byte(sts)))
	req.Header.Set("Authorization", fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		c.ak, scope, strings.Join(signed, ";"), seed))

	// A plausible-looking but wrong chunk signature.
	body := "5;chunk-signature=" + strings.Repeat("0", 64) + "\r\nhello\r\n" +
		"0;chunk-signature=" + strings.Repeat("0", 64) + "\r\n\r\n"

	send, err := http.NewRequest(http.MethodPut, c.base+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	send.Header = req.Header
	resp, err := http.DefaultClient.Do(send)
	if err != nil {
		t.Fatal(err)
	}
	readBody(t, resp)
	if resp.StatusCode == http.StatusOK {
		t.Fatal("a body with forged chunk signatures was accepted")
	}

	// And nothing was stored under that key.
	resp = h.do(request{method: http.MethodGet, path: path})
	expectError(t, resp, http.StatusNotFound, "NoSuchKey")
}
