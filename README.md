# tana

A private S3 object store, plus an agent that puts a WordPress
`wp-content/uploads` directory on top of it without any plugin
noticing.

Everything stays in-house. tana is both the S3 service and the S3
client; there is no third party in the path.

---

## Contents

- [Why](#why)
- [How it works](#how-it-works)
- [Requirements](#requirements)
- [Install](#install)
- [Quick start: one machine](#quick-start-one-machine)
- [Configuration reference](#configuration-reference)
- [NGINX](#nginx)
- [The WordPress plugin](#the-wordpress-plugin)
- [Two machines](#two-machines)
- [Replication](#replication)
- [Day-to-day operation](#day-to-day-operation)
- [Recovering from things going wrong](#recovering-from-things-going-wrong)
- [Troubleshooting](#troubleshooting)
- [What tana does not do](#what-tana-does-not-do)
- [Status](#status)

---

## Why

The usual ways to move WordPress uploads onto object storage each give
something up.

**Offload plugins** rewrite URLs, which works until a plugin calls
`file_exists()` on a real path — and image optimizers, importers and
half the gallery plugins do.

**FUSE-over-S3 mounts** keep the path, but send every `stat()` across
the network. WordPress issues hundreds of metadata calls per request
against a handful of data calls; at 50ms each, regenerating thumbnails
turns into a coffee break.

tana splits the two planes. Metadata is answered locally, in-process,
from an embedded index — so `is_file()` stays true and costs nothing.
Only genuine reads of genuine bytes touch the network, and the store is
on your own LAN, so those cost milliseconds.

---

## How it works

One binary, one config file, and a `roles:` list saying what this
machine is.

**`store`** owns the bytes. Blobs are named after the sha256 of their
content, so identical uploads across sites occupy the disk once, blobs
are immutable and need no locking, and integrity checking is intrinsic
— the filename *is* the checksum. Every mutation is appended to a
journal before it is acknowledged.

**`agent`** runs beside WordPress. It presents the uploads directory as
a FUSE passthrough over a plain backing directory, uploads in the
background, evicts cold objects, and recalls them on read.

A write is acknowledged only after this sequence, in this order:

```
blob durable  →  journal durable  →  index committed  →  ack
```

Every prefix of it is recoverable. A crash after the blob lands leaves
an unreferenced file, which the collector removes. A crash after the
journal entry lands leaves an index that is behind, which is replayed
forward at the next start. There is no ordering in which a client is
told a write succeeded and the bytes are not there.

Eviction removes **only the bytes**. Size, modification time and mode
stay in the index, so an evicted object is indistinguishable from a
present one until somebody actually reads it.

---

## Requirements

- **Linux, amd64 or arm64.** The only supported deployment target.
- **Agent role:** `/dev/fuse`, and either root or `fuse3` installed.
  Linux 6.9 or newer additionally enables FUSE passthrough, which lets
  the kernel serve cached reads without crossing into userspace.
- **Store role:** a filesystem. Put redundancy underneath it — tana
  does no RAID and no erasure coding of its own. ZFS or mdraid.

Check a host before installing:

```bash
tana --caps
```

```
os:          linux/amd64
kernel:      6.8.0-45-generic
root:        true
/dev/fuse:   true
fusermount3: /usr/bin/fusermount3
passthrough: false (needs linux >= 6.9)

agent role: ready
store role: ready
```

---

## Install

Download the tarball for your architecture from the
[releases page](https://github.com/ostap-mykhaylyak/tana/releases),
then:

```bash
tar xzf tana-v0.2.1-linux-amd64.tar.gz && cd tana-v0.2.1-linux-amd64 && sudo ./tana --init
```

`--init` installs the binary in `/sbin/tana`, the systemd unit, a
logrotate policy, and a commented `/etc/tana/config.yaml`.

It will **not** start until you choose its roles. tana cannot invent
your credentials, and a daemon that guessed at them would be worse than
one that refuses.

---

## Quick start: one machine

This is where every deployment should start, including the ones that
will later be split across two hosts. The agent simply talks to
`127.0.0.1` instead of another machine; nothing else differs.

### 1. Generate credentials

```bash
sudo tana --keygen
```

```
access_key: "TANACWOP7IBZHN36BDDXEUXH"
secret_key: "xVtX+fzLAPJhb3oesDkKpnPm9Rpf5lNoxAQIbtmO"
```

### 2. Write the configuration

`/etc/tana/config.yaml`:

```yaml
roles: [store, agent]

store:
  data: /srv/tana
  listen: 127.0.0.1:9200
  region: tana
  buckets:
    - name: shop-uploads
      access_key: "TANACWOP7IBZHN36BDDXEUXH"
      secret_key: "xVtX+fzLAPJhb3oesDkKpnPm9Rpf5lNoxAQIbtmO"
      # Serve media to browsers without credentials.
      public_read: true
      # ...except paid downloads, ever.
      protected:
        - "woocommerce_uploads/**"
  gc:
    interval: 6h
    grace: 72h

agent:
  sites:
    - name: shop.example.com
      uploads: /home/user/public_html/wp-content/uploads
      backing: /var/lib/tana/shop
      cache:
        # Leave this out at first. See the note below.
        max_size: 20GB
        keep_below: 64KB
        never_evict:
          - "woocommerce_uploads/**"
      mount:
        allow_other: true
        populate_user: "www-data"
      backend:
        endpoint: http://127.0.0.1:9200
        bucket: shop-uploads
        access_key: "TANACWOP7IBZHN36BDDXEUXH"
        secret_key: "xVtX+fzLAPJhb3oesDkKpnPm9Rpf5lNoxAQIbtmO"
        ack: remote
```

> **Sizing on one machine.** A cached object exists twice: once in the
> agent's backing store, once as a blob. Eviction still halves that,
> but a cache ceiling buys less here than it does when the store lives
> on another host. Start without `max_size` and add it when you know
> the working set.

### 3. Move the existing uploads

The backing directory is where the real files live. Move what
WordPress already has into it, then leave the mountpoint empty.

```bash
sudo systemctl stop php8.3-fpm nginx
sudo mkdir -p /var/lib/tana/shop
sudo rsync -a /home/user/public_html/wp-content/uploads/ /var/lib/tana/shop/
sudo rm -rf /home/user/public_html/wp-content/uploads/*
```

The backing store must be writable by the account PHP runs as:

```bash
sudo chown -R www-data:www-data /var/lib/tana/shop
```

### 4. Check and start

```bash
sudo tana --check-config
```

```bash
sudo tana start && sudo tana --status
```

```
tana 0.2.1  running  pid 4127  up 3s
roles: store, agent
config: /etc/tana/config.yaml

store  listen 127.0.0.1:9200  data /srv/tana  replica off
  journal seq 0, applied 0
  shop-uploads                        0 obj       0 B total       0 B local  0 dirty

agent
  shop.example.com                 1284 obj    3.1 GiB total    3.1 GiB local  1284 dirty
```

The `dirty` count is the upload backlog. It drains on its own; watch it
go down:

```bash
sudo tana --watch 2s
```

### 5. Verify

```bash
sudo systemctl start php8.3-fpm nginx
```

Load the site, upload an image through the media library, and confirm
it appears in the store:

```bash
sudo tail -f /var/log/tana/transfer.log
```

Then exercise the part that matters — eviction and recall:

```bash
ls -la /home/user/public_html/wp-content/uploads/2026/07/
```

Note a file's size and date, wait for an eviction pass (or lower
`max_size` temporarily), and run `ls -la` again. **The size and date
must be identical.** Then `cat` the file: it comes back.

---

## Configuration reference

The file is read at startup and hot-reloaded when it changes. Some
fields are bound at startup and need a restart; those are marked.

### Top level

| Field | Meaning |
|---|---|
| `roles` | `[store]`, `[agent]`, or both. Restart to change. |

### `store`

| Field | Default | Meaning |
|---|---|---|
| `data` | `/srv/tana` | Root of the blob store. Restart to change. |
| `listen` | `127.0.0.1:9200` | S3 API address. Restart to change. |
| `region` | `tana` | Value expected in the sigv4 credential scope. Arbitrary, but agents must agree. |
| `buckets` | — | The tenant table. Hot-reloadable. |
| `gc.interval` | `6h` | How often unreferenced blobs are swept. |
| `gc.grace` | `72h` | How long an unreferenced blob is kept anyway. |
| `replica.*` | — | See [Replication](#replication). |

#### `store.buckets[]`

| Field | Meaning |
|---|---|
| `name` | Bucket name. Lowercase, 3–63 characters. |
| `access_key` / `secret_key` | The one key pair that may touch this bucket. |
| `public_read` | Allow unauthenticated `GET` and `HEAD`. |
| `protected` | Key patterns excluded from `public_read`, whatever it says. |

One key pair per bucket is the entire authorization model. A model with
one rule is a model nobody misconfigures.

`public_read` never allows writes and never allows listing. Serving a
file is not the same as handing out an index of every file there is.

Patterns: `*` does not cross `/`; a trailing `/**` covers a directory
and everything beneath it, at any depth.

### `agent.sites[]`

| Field | Meaning |
|---|---|
| `name` | Identifies the site in logs, metrics and `--status`. |
| `uploads` | The path WordPress knows. Becomes the FUSE mountpoint. |
| `backing` | Where the real files live. Must not be nested inside `uploads`. |

#### `cache`

| Field | Default | Meaning |
|---|---|---|
| `max_size` | none | Ceiling for the backing store. Omit to mirror everything and evict nothing. |
| `min_free` | none | Keep evicting while the filesystem has less than this free. |
| `keep_below` | `64KB` | Never evict objects smaller than this. |
| `never_evict` | — | Glob patterns pinned by policy. |
| `interval` | `1m` | How often an eviction pass runs. |

`keep_below` exists because thumbnails are what plugins stat and read
constantly. Evicting them frees almost nothing and slows every page;
the bytes worth reclaiming are in the originals nobody has opened since
2019.

#### `mount`

| Field | Default | Meaning |
|---|---|---|
| `allow_other` | `false` | Let users other than the daemon reach the mount. php-fpm and nginx need this whenever tana does not run as them. |
| `populate_user` | none | Reads by this account pull an evicted object back onto disk. Everyone else is streamed through. |
| `debug` | `false` | FUSE protocol tracing. Very loud. |

`populate_user` is the difference between a cache that reflects what
the site uses and one that reflects what the internet asked for. A
crawler walking the 2019 archive would otherwise refill the cache with
exactly the objects that were evicted for being cold. Accepts a name or
a numeric uid.

#### `backend`

| Field | Default | Meaning |
|---|---|---|
| `endpoint` | — | The store's URL. |
| `bucket` | — | Which bucket this site writes to. |
| `region` | store's `region` | Must match the store. |
| `access_key` / `secret_key` | — | The bucket's key pair. |
| `ack` | `remote` | `remote` acknowledges only once the store has the bytes. `local` acknowledges as soon as they are in the backing store, with the upload still queued — faster, and a lie if the machine dies before the queue drains. |

---

## NGINX

There are two distinct jobs, and conflating them is the usual mistake.

### 1. In front of the store, for public media

This is what makes `public_read` useful: browsers and CDNs fetch media
straight from the store, never touching PHP or the FUSE mount.

```nginx
server {
    listen 443 ssl;
    http2 on;
    server_name media.example.com;

    ssl_certificate     /etc/letsencrypt/live/media.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/media.example.com/privkey.pem;

    # Media files are large and nginx has no reason to buffer a whole
    # object before starting to send it.
    proxy_buffering off;

    # An upload arriving through this vhost is a whole media file, and
    # nginx defaults to refusing anything over 1MB.
    client_max_body_size 0;
    proxy_request_buffering off;

    location / {
        # REQUIRED. The sigv4 signature covers the Host header. nginx
        # sends the upstream's name by default, which invalidates every
        # signed request that passes through here. Public reads are
        # unsigned and would survive it; presigned download links would
        # not, and neither would a remote agent.
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        proxy_read_timeout  300s;
        proxy_send_timeout  300s;

        proxy_pass http://127.0.0.1:9200;
    }
}
```

`proxy_set_header Host $host;` is not optional. The signature is
computed over the host the client used; hand the store a different one
and every signed request fails with `SignatureDoesNotMatch`. This is
the single most common way to break a working setup.

**Caching public media.** The store is fast, but a cache in front of it
means a repeated image never wakes the daemon at all:

```nginx
proxy_cache_path /var/cache/nginx/tana levels=1:2 keys_zone=tana:64m
                 max_size=20g inactive=30d use_temp_path=off;

server {
    # ...as above, plus:
    location / {
        proxy_set_header Host $host;
        proxy_pass http://127.0.0.1:9200;

        proxy_cache tana;
        proxy_cache_valid 200 30d;
        proxy_cache_key "$request_uri";
        add_header X-Cache-Status $upstream_cache_status;

        # Never cache anything carrying a query string. Presigned
        # downloads always have one and public media never does, so
        # this is the whole rule — and a cached presigned URL would
        # keep serving a paid file long after its link expired.
        proxy_cache_bypass $is_args;
        proxy_no_cache     $is_args;
    }
}
```

WordPress uploads are write-once — new versions get new filenames — so
a long `proxy_cache_valid` is safe in a way it usually is not.

### 2. The WordPress vhost

Ordinary, with two considerations.

```nginx
server {
    listen 443 ssl;
    http2 on;
    server_name shop.example.com;
    root /home/user/public_html;
    index index.php;

    # WordPress needs to write media through the mount. This is the
    # size of the largest upload you intend to allow, and it must
    # agree with php.ini's upload_max_filesize and post_max_size.
    client_max_body_size 128m;

    location / {
        try_files $uri $uri/ /index.php?$args;
    }

    location ~ \.php$ {
        include snippets/fastcgi-php.conf;
        fastcgi_pass unix:/run/php/php8.3-fpm.sock;
    }

    # With the plugin installed, browsers are sent to media.example.com
    # and never ask for this path. It stays as a fallback for anything
    # that hardcoded a URL — and for the case where you have not
    # installed the plugin at all.
    location ^~ /wp-content/uploads/ {
        # PHP must never execute out of the uploads directory. This is
        # true with or without tana, and it is worth restating because
        # the directory is now a filesystem somebody else controls.
        location ~ \.php$ { deny all; }

        expires 30d;
        add_header Cache-Control "public";

        # open_file_cache holds descriptors to files that eviction may
        # unlink underneath nginx. The content it serves stays correct,
        # but keep the window short so a recalled file is picked up.
        open_file_cache          max=2000 inactive=20s;
        open_file_cache_valid    30s;
        open_file_cache_errors   off;
    }
}
```

Two notes on that last block:

**PHP execution must be denied.** With tana the uploads directory is
served by a filesystem tana controls, which changes nothing about this
rule but makes forgetting it more interesting.

**`open_file_cache_errors off`** matters here. A file that is evicted
and then read comes back; caching the negative result would make it
stay missing for the cache lifetime.

**If you do not install the plugin**, nginx serves uploads from the
mount. That works, and reads by nginx are streamed straight from the
store rather than repopulating the cache — which is exactly what
`populate_user` is for. It is simply slower than sending browsers to
the store directly, and it makes every public request cross the FUSE
layer.

---

## The WordPress plugin

`contrib/tana-mu-plugin.php` is **optional**. tana works without it:
the mount is a real directory and the web server serves from it. The
plugin adds two things worth having.

1. **Public media URLs point at the store**, so public traffic never
   touches the mount at all.
2. **WooCommerce downloads become short-lived signed links** instead of
   files streamed through a PHP-FPM worker. Streaming a large file
   holds that worker for the whole download; a dozen simultaneous
   customers is a shop that has run out of workers.

### Install

```bash
sudo mkdir -p /home/user/public_html/wp-content/mu-plugins
sudo cp contrib/tana-mu-plugin.php /home/user/public_html/wp-content/mu-plugins/
```

Files in `mu-plugins/` load automatically and cannot be deactivated
from wp-admin, which is deliberate: the URL rewriting should not be
switchable by accident.

### Configure

In `wp-config.php`, **above** the `/* That's all, stop editing */`
line:

```php
define('TANA_ENDPOINT',    'https://media.example.com');
define('TANA_BUCKET',      'shop-uploads');
define('TANA_REGION',      'tana');
define('TANA_ACCESS_KEY',  'TANACWOP7IBZHN36BDDXEUXH');
define('TANA_SECRET_KEY',  'xVtX+fzLAPJhb3oesDkKpnPm9Rpf5lNoxAQIbtmO');

// Where browsers fetch public media. Usually the store vhost, or a CDN
// in front of it. No trailing slash.
define('TANA_PUBLIC_BASE', 'https://media.example.com/shop-uploads');

// How long a download link stays valid. Long enough to start a
// download on a slow connection, short enough that a link pasted in a
// forum is worthless by the time anyone clicks it.
define('TANA_PRESIGN_TTL', 300);
```

Missing configuration disables the plugin rather than half-enabling it:
a site serving broken image URLs is worse than a site serving them the
old way.

### What it does

| Hook | Effect |
|---|---|
| `upload_dir` | Rewrites the uploads base URL to `TANA_PUBLIC_BASE`. Attachment URLs, `srcset` entries and everything else follow from this one value. Front end only — wp-admin keeps the local paths. |
| `woocommerce_file_download_method` | Forces `redirect` rather than streaming through PHP. |
| `woocommerce_product_file_download_path` | Replaces the stored path with a presigned URL, minted per request. |

The store refuses `protected` keys to anonymous callers whatever the
plugin does — that rule lives in tana's configuration, where nobody can
switch it off from wp-admin. A signed link is the only way through, and
an expired one does nothing.

> **Verify the WooCommerce integration against your version.** It was
> written against WooCommerce 8.x and 9.x. Place a test order,
> download it, and confirm the link stops working after
> `TANA_PRESIGN_TTL` seconds.

### Checking it works

View the page source of a post with an image. The `src` should point at
`media.example.com`, not at `shop.example.com`.

```bash
curl -sI https://media.example.com/shop-uploads/2026/07/foto.jpg | head -3
```

And confirm a protected file is not reachable without a signature:

```bash
curl -sI https://media.example.com/shop-uploads/woocommerce_uploads/2026/07/manual.pdf | head -1
```

That must be `403`. If it is `200`, the `protected` pattern is not
matching — check it against the key, not against the local path.

---

## Two machines

The split is a configuration change, not a different program.

**On the storage box** (`store01.lan`):

```yaml
roles: [store]
store:
  data: /srv/tana
  listen: 10.0.0.10:9200      # the private interface, not 0.0.0.0
  buckets:
    - name: shop-uploads
      access_key: "TANA..."
      secret_key: "..."
      public_read: true
      protected: ["woocommerce_uploads/**"]
```

**On the web server**:

```yaml
roles: [agent]
agent:
  sites:
    - name: shop.example.com
      uploads: /home/user/public_html/wp-content/uploads
      backing: /var/lib/tana/shop
      cache:
        max_size: 20GB
        min_free: 5GB
      mount:
        allow_other: true
        populate_user: "www-data"
      backend:
        endpoint: http://store01.lan:9200
        bucket: shop-uploads
        access_key: "TANA..."
        secret_key: "..."
```

Put the two on a private network or a WireGuard tunnel. The store's S3
listener has no business facing the internet; the public media vhost in
front of it does.

This is where the cache ceiling starts to matter: the bytes now exist
once locally and once remotely, so eviction genuinely frees the web
server's disk.

---

## Replication

A second store keeps a copy. It **pulls**; the primary does not push.
That means the primary keeps no per-peer state and cannot drift from
what a peer actually has, a secondary that has been down for a week
catches up by asking, and an unreachable peer is the peer's problem
rather than a queue growing on the primary.

What is shipped is the journal already written for crash recovery —
one mechanism rather than two, and the one that exists is exercised on
every restart.

**Primary:**

```yaml
store:
  replica:
    mode: primary
    secret: "a-long-shared-key-from-tana---keygen"
    peers: ["store02.lan"]
```

**Secondary:**

```yaml
store:
  data: /srv/tana
  listen: 10.0.0.11:9200
  buckets:
    # The same bucket table as the primary.
    - name: shop-uploads
      access_key: "TANA..."
      secret_key: "..."
  replica:
    mode: secondary
    secret: "a-long-shared-key-from-tana---keygen"
    from: http://store01.lan:9200
    interval: 10s
```

The secret is not sigv4: replication is not a tenant operation, it
exposes every bucket, and the peer holding the key is your own second
machine. Keep it on the private network.

Lag shows up in `--status`:

```
store  listen 10.0.0.11:9200  data /srv/tana  replica secondary of http://store01.lan:9200, lag 0
```

The secondary keeps its own journal, so it can be promoted by changing
`mode` to `off` and pointing the agents at it — and it can rebuild its
own index without asking anyone.

---

## Day-to-day operation

### Service control

```bash
sudo tana start
```

Also `stop`, `restart`, `reload`, `enable`, `disable`. They act on the
systemd unit. `reload` reopens the log files; the configuration
hot-reloads on its own when the file changes, so a config edit never
needs a restart unless it touched a field marked otherwise.

### Status

```bash
sudo tana --status
```

`--status-json` for machines, `--watch 2s` for a live view. The store's
`journal seq` and `applied` should be equal; a gap after a crash means
the index is still catching up.

### Logs

```
/var/log/tana/tana.log       lifecycle, config reloads, errors
/var/log/tana/access.log     one line per S3 request (buffered)
/var/log/tana/transfer.log   uploads, recalls, evictions, replication
```

All JSON, all rotated by logrotate. Observability is reading files:

```bash
sudo tail -f /var/log/tana/transfer.log | jq -r '"\(.op // .msg) \(.key // "")"'
```

The journal is plain text on purpose too. When a store misbehaves at
three in the morning, `tail` and `jq` are already installed:

```bash
sudo tail -f /srv/tana/journal/*.log | jq -c '{seq, op, key}'
```

### Backups

Back up **`/srv/tana`** (blobs and journal) and
**`/etc/tana/config.yaml`**. That is everything.

`/var/lib/tana/index.db` does not need backing up: it is derived, and
`tana --fsck --rebuild` reconstructs it from the journal.

Blobs are immutable and content-addressed, which makes incremental
backup unusually cheap — nothing is ever rewritten, so a sync only ever
adds files.

### Integrity

```bash
sudo tana stop && sudo tana --fsck && sudo tana start
```

Reads metadata only; seconds even on a large store. It reports missing
blobs (data loss), stale references (bookkeeping) and unreferenced
blobs (the collector's normal backlog), and says which is which.

```bash
sudo tana --scrub
```

Reads every blob and verifies it against its name. Bounded by disk
throughput, so schedule it rather than running it casually. If it finds
corruption, the redundancy under the blob store did not do its job —
check the array before restoring anything.

Both refuse to run while the service is up.

---

## Recovering from things going wrong

### The daemon will not start and the site is down

The backing store is a plain directory of real files at real paths.
That is deliberate:

```bash
sudo umount /home/user/public_html/wp-content/uploads
sudo mount --bind /var/lib/tana/shop /home/user/public_html/wp-content/uploads
```

The site is back in a second, serving everything except objects that
had been evicted. This is the reason the backing store is not an opaque
blob format.

### The index is lost or corrupted

```bash
sudo tana stop && sudo tana --fsck --rebuild && sudo tana start
```

The journal is the durable record; the index never held anything it
does not. On the agent side there is nothing to do at all — the next
scan re-derives it from the backing store.

An index copied between machines with **different page sizes** will
refuse to open with `invalid database`. That is not corruption: bbolt
writes at the page size of the host that created the file, and arm64
kernels ship at 4K, 16K and 64K. Delete it and rebuild; the blobs are
untouched. tana says so in the error.

### Something deleted a lot of files by mistake

Nothing is removed the moment it becomes unreferenced. Blobs wait out
`store.gc.grace` (72h by default), which is exactly what this window is
for. Restore the WordPress database from before the deletion and the
files are still there.

Do not shorten `grace` because the disk looks full.

### A key is gone from the store but the site still expects it

```bash
sudo tana --fsck
```

If it appears under *"object(s) reference content that is not on disk"*,
that is real data loss — restore from the secondary or from backup.
Rebuilding the index will not bring the bytes back, and the tool says
so rather than letting you hope.

---

## Troubleshooting

**`SignatureDoesNotMatch` after putting nginx in front of the store.**
Add `proxy_set_header Host $host;`. The signature covers the host the
client used.

**`RequestTimeTooSkewed`.** The clocks differ by more than fifteen
minutes, which is AWS's window and therefore tana's. Install `chrony`.

**Uploads fail at exactly 1MB.** `client_max_body_size` in nginx. It
defaults to 1MB and refuses anything larger.

**`AccessDenied` on a bucket that exists.** One key pair grants access
to exactly one bucket. Valid credentials for the wrong bucket are still
the wrong credentials.

**A protected file is downloadable without a signature.** The
`protected` pattern is matched against the *object key*, not the local
path. `woocommerce_uploads/**` — no leading slash, no `wp-content`.

**The mount is empty after a restart.** systemd's `MountFlags=shared`
in the unit is what makes the mount visible outside the service's
namespace. If you edited the unit, check it survived.

**php-fpm cannot write to uploads.** The backing directory's ownership
is what governs this. `chown -R www-data:www-data /var/lib/tana/shop`,
and confirm `allow_other: true` is set.

**Everything is slow and `--status` shows a large `dirty` count.** The
upload queue is behind. Check `transfer.log` for the reason; a store
that is refusing writes will say why there.

---

## What tana does not do

Not implemented, and not planned: versioning, lifecycle rules, object
lock, server-side encryption negotiation, bucket policies, ACLs,
website hosting, request payment. Each carries a state machine of its
own and none has anything to do with holding a site's media. They
answer `NotImplemented` by name rather than a silent success a client
would misread.

Also absent by design: RAID, erasure coding, and any other redundancy
below the blob store. That is the filesystem's job, and ZFS does it
better than a first attempt would.

---

## Status

| | |
|---|---|
| **M0** | config, CLI, index, host capability probe |
| **M1** | blob store: content addressing, journal, fsync discipline, refcounts, GC |
| **M2** | S3 API with sigv4, multipart, presigned URLs, public reads |
| **M3** | agent writeback queue and S3 client |
| **M4** | FUSE passthrough mount and recall |
| **M5** | eviction, pinning, caller-aware recall |
| **M6** | journal shipping to a secondary store |
| **M7** | WooCommerce download policy, URL rewriting |
| **M8** | `--fsck`, scrub, index rebuild |

All implemented.

> **One caveat, stated plainly.** `internal/mount` — the FUSE binding —
> compiles and vets for linux/amd64 and linux/arm64, and everything it
> decides is covered by tests through `internal/agent`. But no mount
> has been exercised on a running kernel: development happened on
> Windows. Verify a mount before putting a live site behind one. That
> is why the releases are marked prerelease.

## Licence

MIT. See [LICENSE](LICENSE).
