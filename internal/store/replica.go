package store

import (
	"context"
	"crypto/hmac"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/ostap-mykhaylyak/tana/internal/blob"
	"github.com/ostap-mykhaylyak/tana/internal/config"
	"github.com/ostap-mykhaylyak/tana/internal/index"
	"github.com/ostap-mykhaylyak/tana/internal/journal"
)

// Replication.
//
// The secondary pulls; the primary does not push. That decision buys
// three things. The primary keeps no per-peer state, so it cannot
// drift from what a peer actually has. A secondary that has been down
// for a week catches up by asking, rather than by the primary having
// to remember. And a peer that is unreachable is the peer's problem,
// not a queue growing on the primary.
//
// What is shipped is the journal that is already written for crash
// recovery. Replication and recovery are the same mechanism, so there
// is one code path to get right rather than two, and the one that
// exists is exercised on every restart.

const (
	// replicaAuthScheme prefixes the shared secret in the request.
	replicaAuthScheme = "TANA-REPLICA"
	// replicaBatch is how many records one pull carries.
	replicaBatch = 500
	// replicaPollFloor bounds how often a secondary asks when there is
	// nothing to do.
	replicaPollFloor = time.Second
)

// ReplicaStatus is what --status reports about replication.
type ReplicaStatus struct {
	Mode string `json:"mode"`
	From string `json:"from,omitempty"`
	// Applied is how far this secondary has consumed.
	Applied uint64 `json:"applied,omitempty"`
	// Upstream is the primary's last sequence, as of the last pull.
	Upstream uint64 `json:"upstream,omitempty"`
	// Lag is Upstream minus Applied.
	Lag uint64 `json:"lag,omitempty"`
	// Error is the last failure, if the secondary is not keeping up.
	Error string `json:"error,omitempty"`
	// LastPull is when the secondary last reached its primary.
	LastPull time.Time `json:"last_pull,omitempty"`
}

// journalPage is the reply to a journal pull.
type journalPage struct {
	// Records is the batch, in sequence order.
	Records []journal.Record `json:"records"`
	// Last is the primary's highest sequence, so a secondary can report
	// its lag without a second call.
	Last uint64 `json:"last"`
}

// ReplicaHandler serves the primary side of replication.
//
// It is mounted on the S3 listener under a path S3 cannot produce, and
// guarded by a shared secret rather than sigv4: this is not a tenant
// operation, it exposes every bucket, and the peer holding the secret
// is the operator's own second machine. Put it on a private network.
func (s *Store) ReplicaHandler(secret string) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /-/replica/journal", func(w http.ResponseWriter, r *http.Request) {
		if !replicaAuthorized(r, secret) {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		from, err := strconv.ParseUint(r.URL.Query().Get("from"), 10, 64)
		if err != nil {
			http.Error(w, "malformed from", http.StatusBadRequest)
			return
		}
		page := journalPage{Last: s.jrnl.LastSeq()}
		stop := errors.New("batch full")
		err = s.jrnl.Replay(from, func(rec journal.Record) error {
			if len(page.Records) >= replicaBatch {
				return stop
			}
			page.Records = append(page.Records, rec)
			return nil
		})
		if err != nil && !errors.Is(err, stop) {
			s.svcLog.Error("replica journal read failed", "error", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(page)
	})

	mux.HandleFunc("GET /-/replica/blob/{hash}", func(w http.ResponseWriter, r *http.Request) {
		if !replicaAuthorized(r, secret) {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		hash := r.PathValue("hash")
		if err := blob.ValidHash(hash); err != nil {
			http.Error(w, "malformed hash", http.StatusBadRequest)
			return
		}
		f, err := s.blobs.Open(hash)
		if err != nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		defer f.Close()
		if size, ok, _ := s.blobs.Stat(hash); ok {
			w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		io.Copy(w, f)
	})

	return mux
}

// replicaAuthorized checks the shared secret in constant time.
func replicaAuthorized(r *http.Request, secret string) bool {
	if secret == "" {
		return false // never open the door when no key was configured
	}
	got, ok := cutPrefix(r.Header.Get("Authorization"), replicaAuthScheme+" ")
	if !ok {
		return false
	}
	return hmac.Equal([]byte(got), []byte(secret))
}

func cutPrefix(s, prefix string) (string, bool) {
	if len(s) < len(prefix) || s[:len(prefix)] != prefix {
		return "", false
	}
	return s[len(prefix):], true
}

// Secondary pulls a primary's journal and applies it locally.
type Secondary struct {
	store  *Store
	from   string
	secret string
	http   *http.Client

	status ReplicaStatus
}

// NewSecondary builds the pulling side.
func NewSecondary(s *Store, cfg config.Replica) *Secondary {
	return &Secondary{
		store:  s,
		from:   cfg.From,
		secret: cfg.Secret,
		http:   &http.Client{Timeout: 5 * time.Minute},
		status: ReplicaStatus{Mode: string(config.ReplicaSecondary), From: cfg.From},
	}
}

// Status returns the current replication state.
func (sec *Secondary) Status() ReplicaStatus {
	sec.store.mu.RLock()
	defer sec.store.mu.RUnlock()
	return sec.status
}

// Start pulls on an interval until stop is closed.
func (sec *Secondary) Start(stop <-chan struct{}, interval time.Duration) {
	if interval < replicaPollFloor {
		interval = 10 * time.Second
	}
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			// Pull immediately rather than waiting out the first tick:
			// a secondary that has just started is the one furthest
			// behind.
			if n, err := sec.Pull(context.Background()); err != nil {
				sec.store.svcLog.Error("replication pull failed", "from", sec.from, "error", err)
			} else if n > 0 {
				sec.store.xferLog.Info("replicated", "from", sec.from, "records", n)
			}
			select {
			case <-stop:
				return
			case <-t.C:
			}
		}
	}()
}

// Pull fetches and applies one batch, returning how many records it
// applied. Callers loop until it returns zero to catch up fully.
func (sec *Secondary) Pull(ctx context.Context) (int, error) {
	applied, err := sec.store.appliedSeq()
	if err != nil {
		return 0, err
	}
	page, err := sec.fetchJournal(ctx, applied+1)
	if err != nil {
		sec.record(func(st *ReplicaStatus) { st.Error = err.Error() })
		return 0, err
	}
	sec.record(func(st *ReplicaStatus) {
		st.Error = ""
		st.LastPull = time.Now()
		st.Upstream = page.Last
	})
	if len(page.Records) == 0 {
		sec.record(func(st *ReplicaStatus) { st.Applied = applied; st.Lag = page.Last - applied })
		return 0, nil
	}

	count := 0
	for _, rec := range page.Records {
		if err := sec.applyOne(ctx, rec); err != nil {
			sec.record(func(st *ReplicaStatus) { st.Error = err.Error() })
			return count, err
		}
		count++
	}
	now, _ := sec.store.appliedSeq()
	sec.record(func(st *ReplicaStatus) {
		st.Applied = now
		if page.Last > now {
			st.Lag = page.Last - now
		} else {
			st.Lag = 0
		}
	})
	return count, nil
}

// applyOne mirrors a single record: fetch the blob if this store does
// not have it, write the record to the local journal, then fold it
// into the index.
//
// The order matches the primary's — blob, then journal, then index —
// for the same reason: every prefix of it is recoverable.
func (sec *Secondary) applyOne(ctx context.Context, rec journal.Record) error {
	if rec.Op == journal.OpPut && rec.Hash != "" {
		if _, ok, err := sec.store.blobs.Stat(rec.Hash); err != nil {
			return err
		} else if !ok {
			if err := sec.fetchBlob(ctx, rec.Hash); err != nil {
				return fmt.Errorf("replica: fetch blob %s: %w", rec.Hash, err)
			}
		}
	}
	if err := sec.store.jrnl.AppendAt(rec); err != nil {
		return err
	}
	return sec.store.idx.Update(func(tx *index.Tx) error {
		return sec.store.apply(tx, rec)
	})
}

// fetchJournal asks the primary for records from a sequence onward.
func (sec *Secondary) fetchJournal(ctx context.Context, from uint64) (journalPage, error) {
	url := fmt.Sprintf("%s/-/replica/journal?from=%d", sec.from, from)
	resp, err := sec.get(ctx, url)
	if err != nil {
		return journalPage{}, err
	}
	defer resp.Body.Close()
	var page journalPage
	if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
		return journalPage{}, fmt.Errorf("replica: malformed journal page: %w", err)
	}
	return page, nil
}

// fetchBlob copies one blob from the primary.
func (sec *Secondary) fetchBlob(ctx context.Context, hash string) error {
	resp, err := sec.get(ctx, sec.from+"/-/replica/blob/"+hash)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// blob.Put addresses by content, so a truncated or corrupted
	// transfer lands under a different name and simply fails the check
	// below. There is no way for bad bytes to take a good blob's place.
	info, err := sec.store.blobs.Put(resp.Body)
	if err != nil {
		return err
	}
	if info.Hash != hash {
		return fmt.Errorf("replica: primary served %s under the name %s", info.Hash, hash)
	}
	return nil
}

// get performs an authenticated replication request.
func (sec *Secondary) get(ctx context.Context, url string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", replicaAuthScheme+" "+sec.secret)
	resp, err := sec.http.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
		resp.Body.Close()
		return nil, fmt.Errorf("replica: %s: http %d", url, resp.StatusCode)
	}
	return resp, nil
}

// record updates the status under the store's lock.
func (sec *Secondary) record(fn func(*ReplicaStatus)) {
	sec.store.mu.Lock()
	fn(&sec.status)
	sec.store.mu.Unlock()
}
