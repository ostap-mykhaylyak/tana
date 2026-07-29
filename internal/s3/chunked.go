package s3

import (
	"bufio"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// aws-chunked framing.
//
// When a client streams an upload it cannot hash the body in advance,
// so instead of one signature over the whole payload it sends a chain:
// each chunk is signed using the previous chunk's signature, seeded
// with the request signature. Breaking, reordering or truncating the
// stream breaks the chain.
//
// The alternative — accepting the framing and skipping the chain — is
// common and wrong: it turns an authenticated upload into an
// authenticated request header wrapped around an unauthenticated body.
const (
	chunkAlgorithm = "AWS4-HMAC-SHA256-PAYLOAD"
	// maxChunkSize bounds what one chunk may claim. A chunk is buffered
	// whole in order to verify it, so this is also the memory an upload
	// can cost. Clients in the wild use 64KB to 16MB.
	maxChunkSize = 32 << 20
	// maxTrailers bounds the trailing header section after the final
	// chunk, so a malformed stream cannot be read forever.
	maxTrailers = 16
)

// chunkedReader decodes an aws-chunked body, verifying the signature
// chain when the client signed its chunks.
type chunkedReader struct {
	br     *bufio.Reader
	sig    signature
	key    []byte
	prev   string
	buf    []byte
	off    int
	done   bool
	err    error
	verify bool
}

// newChunkedReader wraps an aws-chunked request body.
func newChunkedReader(body io.Reader, sig signature, region string) *chunkedReader {
	return &chunkedReader{
		br:     bufio.NewReaderSize(body, 64<<10),
		sig:    sig,
		key:    signingKey(sig.creds.SecretKey, sig.date, region),
		prev:   sig.seed,
		verify: sig.chunksSigned(),
	}
}

// Read serves decoded payload bytes.
func (c *chunkedReader) Read(p []byte) (int, error) {
	for c.off >= len(c.buf) {
		if c.err != nil {
			return 0, c.err
		}
		if c.done {
			return 0, io.EOF
		}
		if err := c.nextChunk(); err != nil {
			c.err = err
			return 0, err
		}
	}
	n := copy(p, c.buf[c.off:])
	c.off += n
	return n, nil
}

// nextChunk reads, verifies and buffers one chunk.
func (c *chunkedReader) nextChunk() error {
	header, err := c.readLine()
	if err != nil {
		return err
	}
	// Some clients emit a bare CRLF between chunks.
	if header == "" {
		if header, err = c.readLine(); err != nil {
			return err
		}
	}

	sizeHex, meta, _ := strings.Cut(header, ";")
	size, err := strconv.ParseInt(strings.TrimSpace(sizeHex), 16, 64)
	if err != nil || size < 0 {
		return fmt.Errorf("aws-chunked: malformed chunk size %q", sizeHex)
	}
	if size > maxChunkSize {
		return fmt.Errorf("aws-chunked: chunk of %d bytes exceeds the %d byte limit", size, maxChunkSize)
	}

	var chunkSig string
	if c.verify {
		if chunkSig, err = chunkSignature(meta); err != nil {
			return err
		}
	}

	data := make([]byte, size)
	if _, err := io.ReadFull(c.br, data); err != nil {
		return fmt.Errorf("aws-chunked: short chunk: %w", err)
	}
	// Each chunk is followed by CRLF, which is not payload.
	if _, err := c.readLine(); err != nil {
		return err
	}

	if c.verify {
		expected := c.signChunk(data)
		if !hmac.Equal([]byte(expected), []byte(chunkSig)) {
			return fmt.Errorf("aws-chunked: chunk signature does not match")
		}
		c.prev = expected
	}

	if size == 0 {
		c.done = true
		return c.skipTrailers()
	}
	c.buf, c.off = data, 0
	return nil
}

// signChunk computes a chunk's expected signature, chaining from the
// previous one.
func (c *chunkedReader) signChunk(data []byte) string {
	sum := sha256.Sum256(data)
	sts := strings.Join([]string{
		chunkAlgorithm,
		c.sig.date.UTC().Format(amzDateFormat),
		c.sig.scope,
		c.prev,
		emptyPayloadHash,
		hex.EncodeToString(sum[:]),
	}, "\n")
	return hex.EncodeToString(hmacSHA256(c.key, []byte(sts)))
}

// chunkSignature extracts the signature from a chunk header.
func chunkSignature(meta string) (string, error) {
	sig, ok := strings.CutPrefix(strings.TrimSpace(meta), "chunk-signature=")
	if !ok {
		return "", fmt.Errorf("aws-chunked: chunk is missing its signature")
	}
	return sig, nil
}

// skipTrailers consumes the optional trailing header section. tana
// does not use the checksums carried there — the content hash it
// computes while storing is stronger — but the bytes must be drained
// so the connection can be reused.
func (c *chunkedReader) skipTrailers() error {
	for i := 0; i < maxTrailers; i++ {
		line, err := c.readLine()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if line == "" {
			return nil
		}
	}
	return fmt.Errorf("aws-chunked: too many trailing headers")
}

// readLine reads one CRLF-terminated line without its terminator.
func (c *chunkedReader) readLine() (string, error) {
	line, err := c.br.ReadString('\n')
	if err != nil && line == "" {
		return "", err
	}
	return strings.TrimRight(line, "\r\n"), nil
}
