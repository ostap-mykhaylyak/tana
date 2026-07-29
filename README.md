# tana

A private S3 object store, plus an agent that puts a WordPress
`wp-content/uploads` directory on top of it without any plugin
noticing.

## Why

The usual ways to move WordPress uploads onto object storage each give
something up. Offload plugins rewrite URLs but break every plugin that
expects a real path. FUSE-over-S3 mounts keep the path but send every
`stat()` across the network, and WordPress issues hundreds of metadata
calls per request against a handful of data calls — at 50ms each, a
thumbnail regeneration turns into a coffee break.

tana splits the two planes. Metadata is answered locally, in-process,
from an embedded index. Only actual reads of actual bytes ever touch
the network, and the store sits on your own LAN, so those cost
milliseconds. Nothing is rewritten, nothing is intercepted at the PHP
level, and `is_file()` stays true.

Everything stays in-house: tana is both the S3 client and the S3
service.

## The two roles

One binary, one config file, and a `roles:` list that says what this
machine is.

**`store`** runs on a storage box. It owns the blobs, addresses them by
content hash, writes an append-only journal before acknowledging, and
speaks the subset of the S3 API that WordPress and WooCommerce actually
use.

**`agent`** runs on a web server. It mounts the uploads directory as a
FUSE passthrough over a plain backing directory, keeps a bounded local
cache, uploads in the background, and recalls evicted objects on read.

A small deployment can declare both.

## Requirements

- Linux, amd64 or arm64. This is the only supported deployment target.
- For the agent role: `/dev/fuse`, and either root or `fuse3`
  installed. Linux 6.9 or newer additionally enables FUSE passthrough,
  which lets the kernel serve cached reads without crossing into
  userspace.
- For the store role: a filesystem. Put redundancy underneath it —
  tana does no RAID and no erasure coding of its own.

Check a host with `tana --caps`.

## Install

```
tana --init
```

installs the binary in `/sbin`, the systemd unit, the logrotate policy
and a commented `/etc/tana/config.yaml`. It will not start until you
choose its roles: tana cannot invent your credentials.

```
tana --keygen          # a fresh access key / secret key pair
tana --check-config    # validate before restarting anything
tana start
tana --status
```

## CLI

Service verbs carry no dashes and act on the systemd unit. Everything
else is a flag, so the two can never be confused.

```
tana                     run the daemon in the foreground
tana start|stop|restart|reload|enable|disable

tana --init              install layout, unit, logrotate policy
tana --purge [--yes]     remove config, index and logs (never the blobs)
tana --keygen            print a fresh credential pair
tana --check-config      validate the config, exit non-zero on warnings
tana --caps              report what this host can do
tana --status            query the running daemon
tana --status-json       the same, machine-readable
tana --watch 2s          refresh the status, top-style
tana --version
```

## Layout

```
/etc/tana/config.yaml    configuration, hot-reloaded
/var/lib/tana/index.db   the object index
/var/log/tana/           tana.log, access.log, transfer.log
/run/tana/tana.sock      control socket behind --status
/srv/tana/blobs/ab/cd/…  blobs, named after their sha256
/srv/tana/journal/       one file per segment, newline-delimited JSON
```

The journal is plain text on purpose. When a store misbehaves at three
in the morning, `tail -f` and `jq` are already installed.

## Durability

A write is acknowledged only after this sequence, in this order:

```
blob durable  ->  journal durable  ->  index committed  ->  ack
```

Each step is recoverable from the one before it. A crash after the blob
lands leaves an unreferenced file, which the collector removes once it
is older than the grace period. A crash after the journal entry lands
leaves an index that is behind, which is replayed forward at the next
start. There is no ordering in which a client is told a write succeeded
and the bytes are not there.

Blobs are never deleted the moment they become unreferenced. They wait
out `store.gc.grace`, which is what makes a bulk delete someone regrets
recoverable. The journal is never pruned while it is the only record of
which key holds which content.

Observability is reading log files. Rotation is logrotate's job; the
daemon reopens its streams on SIGHUP.

## Status

Under construction. Milestones:

- [x] **M0** config, CLI, index, host capability probe
- [x] **M1** blob store: content addressing, journal, fsync discipline,
      refcounts, GC
- [ ] **M2** S3 API with sigv4 — the twelve endpoints WordPress needs
- [ ] **M3** agent writeback queue and S3 client
- [ ] **M4** FUSE passthrough mount and recall
- [ ] **M5** eviction, pinning, caller-aware recall
- [ ] **M6** journal shipping to a secondary store
- [ ] **M7** WooCommerce download policy, URL rewriting
- [ ] **M8** `--fsck`, scrub, index rebuild

Not implemented and not planned: versioning, lifecycle rules, object
lock, server-side encryption negotiation, bucket policies. tana is
sized for one job.
