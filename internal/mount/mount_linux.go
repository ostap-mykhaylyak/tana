//go:build linux

// Package mount is the kernel binding: it presents a site's backing
// store at the path WordPress knows, and fills in the objects whose
// bytes have been evicted.
//
// This layer is deliberately thin. Everything it decides — whether an
// object is local, what its metadata is, how to get it back — lives in
// internal/agent, where it can be tested without a mount. What is left
// here is translation between that and the kernel's vocabulary.
//
// The filesystem is a passthrough. For every object whose bytes are on
// disk — which is almost all of them, almost all of the time — the
// loopback implementation underneath handles the call and tana adds
// nothing. Interception happens only for the evicted minority. That is
// the entire difference between this and mounting a bucket: there, the
// answer to "does this file exist" is a network round trip; here it is
// a lookup in a memory-mapped file, and the network is touched only to
// move bytes somebody actually asked to read.
package mount

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path"
	"sync"
	"syscall"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/ostap-mykhaylyak/tana/internal/agent"
	"github.com/ostap-mykhaylyak/tana/internal/index"
)

// attrTimeout is how long the kernel may cache an inode's attributes.
// Long enough to spare the repeated stat storms WordPress produces,
// short enough that an eviction or a recall becomes visible quickly.
const attrTimeout = 5 * time.Second

// Mount is one site's live filesystem.
type Mount struct {
	agent  *agent.Agent
	server *fuse.Server
	log    *slog.Logger

	// phpUID is the user whose reads populate the cache. Everything
	// else is served straight from the store.
	phpUID uint32
	// cacheAll disables the caller check, for deployments where the web
	// server and PHP run as the same user and the distinction is moot.
	cacheAll bool

	once sync.Once
}

// Options tunes a mount.
type Options struct {
	// PopulateUID is the uid whose reads pull an object back into the
	// cache. Zero means every read does.
	PopulateUID uint32
	// AllowOther lets other users reach the mount, which php-fpm and
	// nginx need when tana runs as root.
	AllowOther bool
	// Debug turns on go-fuse's protocol tracing.
	Debug bool
}

// New mounts a site's uploads directory.
func New(ag *agent.Agent, opt Options, log *slog.Logger) (*Mount, error) {
	if err := os.MkdirAll(ag.Uploads(), 0o755); err != nil {
		return nil, fmt.Errorf("mount: create mountpoint %s: %w", ag.Uploads(), err)
	}

	m := &Mount{
		agent:    ag,
		log:      log,
		phpUID:   opt.PopulateUID,
		cacheAll: opt.PopulateUID == 0,
	}

	// The loopback root is built by hand rather than with
	// NewLoopbackRoot, which returns an opaque node: the NewNode hook is
	// the whole point, because it wraps every node the tree ever
	// creates. Interception then applies at any depth without walking
	// the tree ourselves.
	var st syscall.Stat_t
	if err := syscall.Stat(ag.Backing(), &st); err != nil {
		return nil, fmt.Errorf("mount: stat backing store %s: %w", ag.Backing(), err)
	}
	loopback := &gofuse.LoopbackRoot{
		Path: ag.Backing(),
		Dev:  uint64(st.Dev),
	}
	loopback.NewNode = func(root *gofuse.LoopbackRoot, _ *gofuse.Inode, _ string, _ *syscall.Stat_t) gofuse.InodeEmbedder {
		return &node{
			LoopbackNode: gofuse.LoopbackNode{RootData: root},
			mount:        m,
		}
	}
	root := loopback.NewNode(loopback, nil, "", &st)

	sec := time.Second
	server, err := gofuse.Mount(ag.Uploads(), root, &gofuse.Options{
		MountOptions: fuse.MountOptions{
			AllowOther: opt.AllowOther,
			Debug:      opt.Debug,
			FsName:     "tana:" + ag.Name(),
			Name:       "tana",
			// The uploads directory is served to a web server; nothing
			// under it should ever be executable or setuid.
			Options: []string{"nosuid", "nodev"},
		},
		AttrTimeout:  &sec,
		EntryTimeout: &sec,
	})
	if err != nil {
		return nil, fmt.Errorf("mount %s: %w", ag.Uploads(), err)
	}
	m.server = server
	log.Info("mounted", "site", ag.Name(), "at", ag.Uploads(), "backing", ag.Backing())
	return m, nil
}

// Wait blocks until the filesystem is unmounted.
func (m *Mount) Wait() { m.server.Wait() }

// Unmount detaches the filesystem.
func (m *Mount) Unmount() error {
	var err error
	m.once.Do(func() {
		err = m.server.Unmount()
		if err == nil {
			m.log.Info("unmounted", "site", m.agent.Name(), "at", m.agent.Uploads())
		}
	})
	return err
}

// node is one file or directory. It is a loopback node until the
// object it names turns out to be evicted.
type node struct {
	gofuse.LoopbackNode
	mount *Mount
}

var (
	_ gofuse.NodeLookuper  = (*node)(nil)
	_ gofuse.NodeOpener    = (*node)(nil)
	_ gofuse.NodeGetattrer = (*node)(nil)
	_ gofuse.NodeReaddirer = (*node)(nil)
)

// key returns the object key this node corresponds to.
func (n *node) key() string {
	return n.Path(n.Root())
}

// Lookup resolves a name, falling back to the index when the file is
// not on disk.
//
// This is the call that makes eviction invisible. WordPress asks
// whether a file exists far more often than it reads one, and the
// answer here comes from the index rather than the network.
func (n *node) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*gofuse.Inode, syscall.Errno) {
	child, errno := n.LoopbackNode.Lookup(ctx, name, out)
	if errno != syscall.ENOENT {
		return child, errno
	}

	key := path.Join(n.key(), name)
	e, ok, err := n.mount.agent.Entry(key)
	if err != nil || !ok || e.State.Local() {
		return nil, syscall.ENOENT
	}

	stable := gofuse.StableAttr{Mode: fuse.S_IFREG, Ino: inodeOf(key)}
	fillAttr(&out.Attr, e)
	out.SetEntryTimeout(attrTimeout)
	out.SetAttrTimeout(attrTimeout)
	return n.NewInode(ctx, &node{
		LoopbackNode: gofuse.LoopbackNode{RootData: n.RootData},
		mount:        n.mount,
	}, stable), 0
}

// Getattr answers stat(2). For an evicted object the answer comes from
// the index and is exactly what the file would report if it were here.
func (n *node) Getattr(ctx context.Context, f gofuse.FileHandle, out *fuse.AttrOut) syscall.Errno {
	errno := n.LoopbackNode.Getattr(ctx, f, out)
	if errno != syscall.ENOENT {
		if errno == 0 {
			out.SetTimeout(attrTimeout)
		}
		return errno
	}
	e, ok, err := n.mount.agent.Entry(n.key())
	if err != nil || !ok {
		return syscall.ENOENT
	}
	fillAttr(&out.Attr, e)
	out.SetTimeout(attrTimeout)
	return 0
}

// Readdir lists a directory, adding back the objects whose bytes are
// gone. Without this a plugin scanning the directory would conclude
// half the media library had been deleted.
func (n *node) Readdir(ctx context.Context) (gofuse.DirStream, syscall.Errno) {
	local, errno := n.LoopbackNode.Readdir(ctx)
	if errno != 0 {
		return local, errno
	}
	evicted, err := n.mount.agent.EvictedChildren(n.key())
	if err != nil || len(evicted) == 0 {
		return local, 0
	}

	var entries []fuse.DirEntry
	seen := map[string]bool{}
	for local.HasNext() {
		d, errno := local.Next()
		if errno != 0 {
			local.Close()
			return nil, errno
		}
		seen[d.Name] = true
		entries = append(entries, d)
	}
	local.Close()

	for name, e := range evicted {
		if seen[name] {
			continue
		}
		mode := uint32(fuse.S_IFREG)
		if fs.FileMode(e.Mode).IsDir() {
			mode = fuse.S_IFDIR
		}
		entries = append(entries, fuse.DirEntry{
			Name: name, Mode: mode, Ino: inodeOf(e.Key),
		})
	}
	return gofuse.NewListDirStream(entries), 0
}

// Open opens a file, recalling it first when its bytes are elsewhere.
//
// Who is asking decides what happens. A read from PHP is the site's
// own code and predicts more reads, so the object comes back to disk.
// A read from the web server is public traffic — a crawler walking the
// 2019 archive — and is served straight through, because letting it
// populate the cache would undo the eviction that just made room.
func (n *node) Open(ctx context.Context, flags uint32) (gofuse.FileHandle, uint32, syscall.Errno) {
	key := n.key()
	e, known, err := n.mount.agent.Entry(key)
	if err != nil || !known || e.State.Local() {
		fh, fuseFlags, errno := n.LoopbackNode.Open(ctx, flags)
		if errno == 0 && known {
			n.mount.agent.Touch(key)
		}
		return fh, fuseFlags, errno
	}

	// A write to an evicted object has to recall it first: the caller
	// may be appending, and truncating it to nothing would lose data.
	writing := flags&(syscall.O_WRONLY|syscall.O_RDWR|syscall.O_APPEND) != 0

	if writing || n.mount.shouldPopulate(ctx) {
		if err := n.mount.agent.Recall(ctx, key); err != nil {
			n.mount.log.Error("recall failed", "site", n.mount.agent.Name(), "key", key, "error", err)
			return nil, 0, syscall.EIO
		}
		n.mount.agent.Touch(key)
		return n.LoopbackNode.Open(ctx, flags)
	}

	n.mount.agent.Touch(key)
	return &remoteFile{mount: n.mount, key: key, size: e.Size}, fuse.FOPEN_KEEP_CACHE, 0
}

// shouldPopulate reports whether this caller's read should pull the
// object back onto local disk.
func (m *Mount) shouldPopulate(ctx context.Context) bool {
	if m.cacheAll {
		return true
	}
	caller, ok := fuse.FromContext(ctx)
	if !ok {
		return true // unknown caller: behave as before
	}
	return caller.Uid == m.phpUID
}

// remoteFile serves reads straight from the store, without landing the
// object on disk.
type remoteFile struct {
	mount *Mount
	key   string
	size  int64
}

var (
	_ gofuse.FileReader    = (*remoteFile)(nil)
	_ gofuse.FileGetattrer = (*remoteFile)(nil)
)

func (f *remoteFile) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	if off >= f.size {
		return fuse.ReadResultData(nil), 0
	}
	if int64(len(dest)) > f.size-off {
		dest = dest[:f.size-off]
	}
	n, err := f.mount.agent.ReadRemoteAt(ctx, f.key, dest, off)
	if err != nil {
		f.mount.log.Error("streaming read failed",
			"site", f.mount.agent.Name(), "key", f.key, "offset", off, "error", err)
		return nil, syscall.EIO
	}
	return fuse.ReadResultData(dest[:n]), 0
}

func (f *remoteFile) Getattr(ctx context.Context, out *fuse.AttrOut) syscall.Errno {
	e, ok, err := f.mount.agent.Entry(f.key)
	if err != nil || !ok {
		return syscall.ENOENT
	}
	fillAttr(&out.Attr, e)
	out.SetTimeout(attrTimeout)
	return 0
}

// fillAttr renders an index entry as the kernel's stat structure.
// Size, times and mode all come from the index, which is why an
// evicted object is indistinguishable from a present one until
// somebody reads it.
func fillAttr(a *fuse.Attr, e index.Entry) {
	mode := e.Mode
	if mode == 0 {
		mode = 0o644
	}
	a.Mode = fuse.S_IFREG | (mode & 0o7777)
	if fs.FileMode(e.Mode).IsDir() {
		a.Mode = fuse.S_IFDIR | (mode & 0o7777)
	}
	a.Size = uint64(e.Size)
	// The kernel reports allocated blocks; an evicted object occupies
	// none, and reporting otherwise would make du(1) lie in the
	// direction that matters least.
	a.Blocks = 0
	a.Nlink = 1
	sec := uint64(e.ModTime.Unix())
	a.Mtime, a.Ctime, a.Atime = sec, sec, sec
	a.Ino = inodeOf(e.Key)
}

// inodeOf derives a stable inode number from a key.
//
// It must be stable across restarts: a program holding a directory
// listing across an eviction would otherwise see the file change
// identity underneath it. FNV-1a over the key gives that for free, and
// a collision costs nothing here — the kernel uses the number to
// cache, not to address.
func inodeOf(key string) uint64 {
	const (
		offset = 14695981039346656037
		prime  = 1099511628211
	)
	h := uint64(offset)
	for i := 0; i < len(key); i++ {
		h ^= uint64(key[i])
		h *= prime
	}
	// Never zero: zero is not a valid inode number.
	if h == 0 {
		return 1
	}
	return h
}
