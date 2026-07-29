// Package journal is the append-only record of every mutation the
// store makes.
//
// It is written for crash recovery — the index is a derived structure,
// and replaying the journal rebuilds it — and the same file is what a
// secondary consumes to stay in step. Replication and recovery being
// the same mechanism is the point: two mechanisms would mean two
// chances to get durability subtly wrong, and only one of them would
// be exercised often enough to notice.
//
// The format is newline-delimited JSON, one record per line, in
// segment files named after the first sequence number they contain. It
// is not the most compact choice, and that is deliberate: when a store
// misbehaves at three in the morning, `tail -f` and `jq` are the tools
// that are already installed.
//
// A record is durable when Append returns. That costs an fsync per
// mutation, which is the price of being allowed to say a write
// happened.
package journal

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Op is the kind of mutation a record describes.
type Op string

const (
	// OpPut binds a key to a blob.
	OpPut Op = "put"
	// OpDelete unbinds a key.
	OpDelete Op = "delete"
	// OpGC records that a blob was collected, so a secondary can drop
	// it too instead of rediscovering the fact.
	OpGC Op = "gc"
)

const (
	segmentSuffix = ".log"
	// segmentDigits pads segment names so lexical order is numeric
	// order, which is what makes a plain `ls` readable.
	segmentDigits = 12

	dirPerm  = 0o750
	filePerm = 0o640
)

// maxSegment bounds one file, so pruning can drop whole files and
// never has to rewrite one. A variable rather than a constant so tests
// can exercise rotation without writing 64MB.
var maxSegment int64 = 64 << 20

// Record is one mutation. Every field a replay needs is present, so
// applying a record is idempotent: it never has to consult the state
// it is about to overwrite.
type Record struct {
	Seq    uint64    `json:"seq"`
	Time   time.Time `json:"time"`
	Op     Op        `json:"op"`
	Bucket string    `json:"bucket,omitempty"`
	Key    string    `json:"key,omitempty"`
	Hash   string    `json:"hash,omitempty"`
	// ETag is the S3 entity tag the client was given. It is recorded so
	// a replay hands back the same value the client already stored;
	// recomputing it would be impossible for multipart objects, whose
	// tag depends on how the upload happened to be split.
	ETag  string    `json:"etag,omitempty"`
	Size  int64     `json:"size,omitempty"`
	MTime time.Time `json:"mtime,omitempty"`
}

// Journal is an append-only sequence of records across segment files.
type Journal struct {
	dir string

	mu      sync.Mutex
	cur     *os.File
	curSize int64
	curBase uint64 // first seq in the open segment
	last    uint64 // highest seq written
}

// Open prepares the journal in dir, recovering from an interrupted
// append if there was one.
func Open(dir string) (*Journal, error) {
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return nil, fmt.Errorf("journal: create %s: %w", dir, err)
	}
	j := &Journal{dir: dir}
	segs, err := j.segments()
	if err != nil {
		return nil, err
	}
	if len(segs) == 0 {
		return j, nil
	}
	// Only the final record of the final segment can be torn: every
	// earlier one was followed by a successful fsync.
	lastSeg := segs[len(segs)-1]
	last, err := j.repairTail(lastSeg)
	if err != nil {
		return nil, err
	}
	j.last = last
	return j, nil
}

// Dir returns the journal directory.
func (j *Journal) Dir() string { return j.dir }

// LastSeq returns the highest sequence number written.
func (j *Journal) LastSeq() uint64 {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.last
}

// Append assigns the next sequence number, writes the record and
// flushes it to stable storage. The assigned number is returned.
func (j *Journal) Append(r Record) (uint64, error) {
	j.mu.Lock()
	defer j.mu.Unlock()

	if r.Time.IsZero() {
		r.Time = time.Now().UTC()
	}
	r.Seq = j.last + 1

	line, err := json.Marshal(r)
	if err != nil {
		return 0, fmt.Errorf("journal: encode: %w", err)
	}
	line = append(line, '\n')

	if err := j.ensureSegment(r.Seq, int64(len(line))); err != nil {
		return 0, err
	}
	n, err := j.cur.Write(line)
	if err != nil {
		return 0, fmt.Errorf("journal: write: %w", err)
	}
	j.curSize += int64(n)
	if err := j.cur.Sync(); err != nil {
		return 0, fmt.Errorf("journal: sync: %w", err)
	}
	j.last = r.Seq
	return r.Seq, nil
}

// ensureSegment opens the current segment, rotating when the next
// record would push it past maxSegment. Callers hold the lock.
func (j *Journal) ensureSegment(nextSeq uint64, need int64) error {
	if j.cur != nil && j.curSize+need <= maxSegment {
		return nil
	}
	if j.cur != nil {
		if err := j.cur.Close(); err != nil {
			return fmt.Errorf("journal: close segment: %w", err)
		}
		j.cur = nil
	}
	// Reopening the newest existing segment rather than always
	// starting a new one keeps a restart from littering the directory
	// with one tiny file per run.
	segs, err := j.segments()
	if err != nil {
		return err
	}
	var path string
	var base uint64
	if len(segs) > 0 {
		newest := segs[len(segs)-1]
		if fi, err := os.Stat(newest.path); err == nil && fi.Size()+need <= maxSegment {
			path, base = newest.path, newest.base
		}
	}
	if path == "" {
		base = nextSeq
		path = filepath.Join(j.dir, segmentName(base))
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, filePerm)
	if err != nil {
		return fmt.Errorf("journal: open segment: %w", err)
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return fmt.Errorf("journal: stat segment: %w", err)
	}
	// A new segment file needs its directory entry flushed too, or a
	// power cut can lose the whole segment along with its records.
	if fi.Size() == 0 {
		if err := syncDir(j.dir); err != nil {
			f.Close()
			return fmt.Errorf("journal: sync dir: %w", err)
		}
	}
	j.cur, j.curSize, j.curBase = f, fi.Size(), base
	return nil
}

// Replay calls fn for every record with Seq >= from, in order.
func (j *Journal) Replay(from uint64, fn func(Record) error) error {
	segs, err := j.segments()
	if err != nil {
		return err
	}
	for _, seg := range segs {
		// Segments are named after their first sequence number, so one
		// whose successor starts at or below `from` holds nothing new.
		if seg.next != 0 && seg.next <= from {
			continue
		}
		if err := replayFile(seg.path, from, fn); err != nil {
			return err
		}
	}
	return nil
}

// replayFile streams one segment.
func replayFile(path string, from uint64, fn func(Record) error) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("journal: open %s: %w", path, err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4<<20)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var r Record
		if err := json.Unmarshal(line, &r); err != nil {
			return fmt.Errorf("journal: corrupt record in %s: %w", path, err)
		}
		if r.Seq < from {
			continue
		}
		if err := fn(r); err != nil {
			return err
		}
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("journal: read %s: %w", path, err)
	}
	return nil
}

// Prune removes whole segments that contain nothing at or above
// `keepFrom`. Segments are never rewritten: a journal you only ever
// append to and delete from cannot be corrupted by pruning.
func (j *Journal) Prune(keepFrom uint64) (int, error) {
	j.mu.Lock()
	defer j.mu.Unlock()

	segs, err := j.segments()
	if err != nil {
		return 0, err
	}
	removed := 0
	for _, seg := range segs {
		if seg.next == 0 || seg.next > keepFrom {
			continue // still holds live records, or is the open one
		}
		if j.cur != nil && seg.base == j.curBase {
			continue
		}
		if err := os.Remove(seg.path); err != nil {
			return removed, fmt.Errorf("journal: prune %s: %w", seg.path, err)
		}
		removed++
	}
	return removed, nil
}

// Close flushes and closes the open segment.
func (j *Journal) Close() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.cur == nil {
		return nil
	}
	err := j.cur.Close()
	j.cur = nil
	return err
}

// segment is one file plus the sequence range it covers. next is the
// first sequence of the following segment, or 0 for the newest one.
type segment struct {
	path string
	base uint64
	next uint64
}

// segments lists the segment files in sequence order.
func (j *Journal) segments() ([]segment, error) {
	entries, err := os.ReadDir(j.dir)
	if err != nil {
		return nil, fmt.Errorf("journal: read %s: %w", j.dir, err)
	}
	var out []segment
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), segmentSuffix) {
			continue
		}
		base, err := strconv.ParseUint(strings.TrimSuffix(e.Name(), segmentSuffix), 10, 64)
		if err != nil {
			continue // not ours
		}
		out = append(out, segment{path: filepath.Join(j.dir, e.Name()), base: base})
	}
	sort.Slice(out, func(a, b int) bool { return out[a].base < out[b].base })
	for i := 0; i < len(out)-1; i++ {
		out[i].next = out[i+1].base
	}
	return out, nil
}

// repairTail reads the newest segment to find the highest usable
// sequence number, truncating a torn final record if the process died
// mid-append.
//
// Only the last line can be torn, because every earlier one was
// followed by a successful fsync. A parse failure anywhere else is
// real corruption and is reported rather than swallowed.
func (j *Journal) repairTail(seg segment) (uint64, error) {
	f, err := os.OpenFile(seg.path, os.O_RDWR, filePerm)
	if err != nil {
		return 0, fmt.Errorf("journal: open %s: %w", seg.path, err)
	}
	defer f.Close()

	var (
		last   uint64
		offset int64 // end of the last complete, valid record
	)
	rd := bufio.NewReader(f)
	for {
		line, err := rd.ReadBytes('\n')
		if len(line) > 0 && line[len(line)-1] == '\n' {
			var r Record
			if jsonErr := json.Unmarshal(line[:len(line)-1], &r); jsonErr != nil {
				return 0, fmt.Errorf("journal: corrupt record in %s at offset %d: %w", seg.path, offset, jsonErr)
			}
			last = r.Seq
			offset += int64(len(line))
			continue
		}
		// A trailing fragment with no newline: the append that wrote it
		// never completed, so no caller was ever told it succeeded.
		if len(line) > 0 {
			if err := f.Truncate(offset); err != nil {
				return 0, fmt.Errorf("journal: truncate torn record in %s: %w", seg.path, err)
			}
			if err := f.Sync(); err != nil {
				return 0, fmt.Errorf("journal: sync after truncate: %w", err)
			}
		}
		if errors.Is(err, io.EOF) {
			return last, nil
		}
		if err != nil {
			return 0, fmt.Errorf("journal: read %s: %w", seg.path, err)
		}
		return last, nil
	}
}

// segmentName renders a segment file name from its first sequence.
func segmentName(base uint64) string {
	return fmt.Sprintf("%0*d%s", segmentDigits, base, segmentSuffix)
}

// AppendAt writes a record that already carries its sequence number,
// which is what a secondary does with what it pulls from its primary.
//
// The sequence must be exactly the next one. Replication that could
// skip a record would be replication that silently diverges, and a
// gap is far easier to refuse than to detect later.
func (j *Journal) AppendAt(r Record) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	if r.Seq != j.last+1 {
		return fmt.Errorf("journal: out of order record: got seq %d, expected %d", r.Seq, j.last+1)
	}
	if r.Time.IsZero() {
		r.Time = time.Now().UTC()
	}
	line, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("journal: encode: %w", err)
	}
	line = append(line, '\n')

	if err := j.ensureSegment(r.Seq, int64(len(line))); err != nil {
		return err
	}
	n, err := j.cur.Write(line)
	if err != nil {
		return fmt.Errorf("journal: write: %w", err)
	}
	j.curSize += int64(n)
	if err := j.cur.Sync(); err != nil {
		return fmt.Errorf("journal: sync: %w", err)
	}
	j.last = r.Seq
	return nil
}
