// Package config loads and validates the tana configuration
// (/etc/tana/config.yaml) and provides hot-reload via fsnotify.
//
// One file describes the whole machine. What the daemon actually runs
// is decided by roles: a storage box declares [store], a web server
// declares [agent], and a single-box deployment declares both.
//
// Validation distinguishes two severities. A structural problem (no
// roles, a store without buckets, a site without a backend) is fatal:
// the daemon refuses to start rather than run half-configured. A
// skippable problem (one malformed bucket in a list of ten) drops that
// entry and records a Warning, so one typo cannot take down every site
// on the machine.
package config

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/ostap-mykhaylyak/tana/internal/paths"
)

// Role is a subsystem the daemon runs on this machine.
type Role string

const (
	// RoleStore is the S3 service: it owns the blobs.
	RoleStore Role = "store"
	// RoleAgent is the WordPress-side agent: FUSE mount, cache,
	// writeback. It owns nothing durable.
	RoleAgent Role = "agent"
)

// Ack selects when a write is acknowledged to WordPress.
type Ack string

const (
	// AckRemote acknowledges only once the store has the blob on disk.
	// Slower and correct.
	AckRemote Ack = "remote"
	// AckLocal acknowledges as soon as the file is in the backing
	// store, with the upload still queued. Faster, and a lie if the
	// machine dies before the queue drains.
	AckLocal Ack = "local"
)

// ReplicaMode is the store's role in journal shipping.
type ReplicaMode string

const (
	ReplicaOff       ReplicaMode = "off"
	ReplicaPrimary   ReplicaMode = "primary"
	ReplicaSecondary ReplicaMode = "secondary"
)

// Duration wraps time.Duration to accept human-friendly YAML values
// such as "30m", "6h", plus a whole-days form ("7d") that
// time.ParseDuration lacks — retention windows are naturally in days.
type Duration time.Duration

// UnmarshalYAML implements yaml.Unmarshaler via time.ParseDuration.
func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	if days, ok := strings.CutSuffix(s, "d"); ok {
		n, err := strconv.Atoi(days)
		if err != nil || n < 0 {
			return fmt.Errorf("invalid duration %q", s)
		}
		*d = Duration(time.Duration(n) * 24 * time.Hour)
		return nil
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*d = Duration(v)
	return nil
}

// MarshalYAML renders the duration back in its string form.
func (d Duration) MarshalYAML() (any, error) { return time.Duration(d).String(), nil }

// Std returns the value as a standard time.Duration.
func (d Duration) Std() time.Duration { return time.Duration(d) }

// Size is a byte quantity written the way operators think about disks:
// "20GB", "512MB", "64KB". Every suffix is 1024-based (KiB/MiB/GiB are
// accepted as explicit spellings of the same thing) because the number
// is always compared against a filesystem, and filesystems report in
// powers of two. A bare number is bytes.
type Size int64

var sizeUnits = []struct {
	suffix string
	mult   int64
}{
	{"TIB", 1 << 40}, {"GIB", 1 << 30}, {"MIB", 1 << 20}, {"KIB", 1 << 10},
	{"TB", 1 << 40}, {"GB", 1 << 30}, {"MB", 1 << 20}, {"KB", 1 << 10},
	{"T", 1 << 40}, {"G", 1 << 30}, {"M", 1 << 20}, {"K", 1 << 10},
	{"B", 1},
}

// ParseSize parses a size literal. Exported for reuse by the CLI flags
// (--evict --above 100MB).
func ParseSize(s string) (Size, error) {
	t := strings.ToUpper(strings.TrimSpace(s))
	if t == "" {
		return 0, fmt.Errorf("empty size")
	}
	for _, u := range sizeUnits {
		num, ok := strings.CutSuffix(t, u.suffix)
		if !ok {
			continue
		}
		num = strings.TrimSpace(num)
		v, err := strconv.ParseFloat(num, 64)
		if err != nil || v < 0 {
			return 0, fmt.Errorf("invalid size %q", s)
		}
		return Size(v * float64(u.mult)), nil
	}
	v, err := strconv.ParseInt(t, 10, 64)
	if err != nil || v < 0 {
		return 0, fmt.Errorf("invalid size %q", s)
	}
	return Size(v), nil
}

// UnmarshalYAML accepts both a string ("20GB") and a bare integer.
func (z *Size) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	v, err := ParseSize(s)
	if err != nil {
		return err
	}
	*z = v
	return nil
}

// MarshalYAML renders the size back in a compact human form.
func (z Size) MarshalYAML() (any, error) { return z.String(), nil }

// String renders the size using the largest unit that divides it
// exactly, so a round-trip through YAML stays readable.
func (z Size) String() string {
	n := int64(z)
	for _, u := range []struct {
		suffix string
		mult   int64
	}{{"TB", 1 << 40}, {"GB", 1 << 30}, {"MB", 1 << 20}, {"KB", 1 << 10}} {
		if n >= u.mult && n%u.mult == 0 {
			return strconv.FormatInt(n/u.mult, 10) + u.suffix
		}
	}
	return strconv.FormatInt(n, 10) + "B"
}

// Bytes returns the value as a plain int64.
func (z Size) Bytes() int64 { return int64(z) }

// Config is the whole machine configuration. Every field has a
// production default (see applyDefaults), so config.yaml may be sparse.
type Config struct {
	// Roles lists the subsystems to run: [store], [agent], or both.
	Roles []Role `yaml:"roles"`

	Store Store `yaml:"store"`
	Agent Agent `yaml:"agent"`

	// Warnings collects non-fatal issues found by validate()
	// (e.g. list entries that were skipped). Never fatal.
	Warnings []string `yaml:"-"`
}

// Store configures the S3 service: the side that owns the bytes.
type Store struct {
	// Data is the root of the blob store. Blobs live under
	// data/blobs/ab/cd/<sha256>, the journal under data/journal.
	Data string `yaml:"data"`

	// Listen is the S3 API address. Bind it to the private interface:
	// nothing in tana's threat model expects the raw store to face the
	// internet.
	Listen string `yaml:"listen"`

	// Region is the value returned in the S3 API and expected in the
	// sigv4 credential scope. Arbitrary, but clients must agree.
	Region string `yaml:"region"`

	// Buckets is the tenant table: one entry per site, each with its
	// own credentials.
	Buckets []Bucket `yaml:"buckets"`

	GC      GC      `yaml:"gc"`
	Replica Replica `yaml:"replica"`
}

// Bucket is one S3 bucket and the key pair that may access it.
type Bucket struct {
	Name      string `yaml:"name"`
	AccessKey string `yaml:"access_key"`
	SecretKey string `yaml:"secret_key"`
}

// GC configures the sweep that reclaims blobs nobody references any
// more. Deletion is never immediate: a blob that just lost its last
// reference stays for Grace, which is what makes an accidental mass
// delete recoverable.
type GC struct {
	Interval Duration `yaml:"interval"`
	Grace    Duration `yaml:"grace"`
}

// Replica configures journal shipping between stores. The journal is
// already written for crash recovery, so replication reuses it rather
// than introducing a second mechanism.
type Replica struct {
	Mode ReplicaMode `yaml:"mode"`
	// Peers are the secondaries a primary ships to.
	Peers []string `yaml:"peers"`
	// From is the primary a secondary pulls from.
	From string `yaml:"from"`
}

// Agent configures the WordPress-side agent: one entry per site.
type Agent struct {
	Sites []Site `yaml:"sites"`
}

// Site is one WordPress uploads directory placed under tana.
type Site struct {
	// Name identifies the site in logs, metrics and --status.
	Name string `yaml:"name"`

	// Uploads is the path WordPress knows: the FUSE mountpoint.
	Uploads string `yaml:"uploads"`

	// Backing is where the real files live. Keeping it a plain
	// directory of real files at real paths is deliberate: if the
	// daemon dies you can bind-mount it over Uploads and the site is
	// back, minus the evicted objects.
	Backing string `yaml:"backing"`

	Cache   Cache   `yaml:"cache"`
	Backend Backend `yaml:"backend"`
	Mount   Mount   `yaml:"mount"`
}

// Mount tunes the FUSE filesystem for one site.
type Mount struct {
	// AllowOther lets users other than the daemon's own reach the
	// mount. php-fpm and the web server need it whenever tana does not
	// run as them, which is almost always.
	AllowOther bool `yaml:"allow_other"`
	// PopulateUser names the account whose reads pull an evicted object
	// back onto local disk — normally the PHP-FPM pool user. Reads by
	// anyone else, meaning public traffic through the web server, are
	// streamed from the store instead. Without this a crawler walking
	// an old archive refills the cache with exactly the objects that
	// were evicted for being cold. Empty means every read populates.
	PopulateUser string `yaml:"populate_user"`
	// Debug turns on FUSE protocol tracing. Very loud.
	Debug bool `yaml:"debug"`
}

// Cache bounds how much of the site stays on local disk.
type Cache struct {
	// MaxSize is the ceiling for the backing store.
	MaxSize Size `yaml:"max_size"`
	// MinFree keeps eviction going while the filesystem is below this
	// much free space, regardless of MaxSize.
	MinFree Size `yaml:"min_free"`
	// KeepBelow never evicts objects smaller than this. Thumbnails are
	// what plugins stat and read constantly, and they cost almost
	// nothing to keep.
	KeepBelow Size `yaml:"keep_below"`
	// NeverEvict are glob patterns, relative to Uploads, that are
	// pinned by policy. A trailing /** covers a directory and
	// everything beneath it, at any depth.
	NeverEvict []string `yaml:"never_evict"`
	// Interval is how often an eviction pass runs. Passes are cheap
	// when there is nothing to do — they read counters, not the disk.
	Interval Duration `yaml:"interval"`
}

// Backend is the store this site writes to.
type Backend struct {
	Endpoint  string `yaml:"endpoint"`
	Bucket    string `yaml:"bucket"`
	Region    string `yaml:"region"`
	AccessKey string `yaml:"access_key"`
	SecretKey string `yaml:"secret_key"`
	Ack       Ack    `yaml:"ack"`
}

// Default returns the built-in configuration.
func Default() *Config {
	return &Config{
		Roles: nil,
		Store: Store{
			Data:   paths.DefaultStoreData,
			Listen: "127.0.0.1:9200",
			Region: "tana",
			GC:     GC{Interval: Duration(6 * time.Hour), Grace: Duration(72 * time.Hour)},
			Replica: Replica{
				Mode: ReplicaOff,
			},
		},
	}
}

// Load reads, parses and validates the configuration at path.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	cfg := Default()
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("invalid %s: %w", path, err)
	}
	return cfg, nil
}

// Has reports whether the daemon should run the given role.
func (c *Config) Has(r Role) bool {
	for _, got := range c.Roles {
		if got == r {
			return true
		}
	}
	return false
}

// applyDefaults fills in what a sparse file left empty. It runs before
// validate so validation sees the effective values.
func (c *Config) applyDefaults() {
	d := Default()
	if c.Store.Data == "" {
		c.Store.Data = d.Store.Data
	}
	if c.Store.Listen == "" {
		c.Store.Listen = d.Store.Listen
	}
	if c.Store.Region == "" {
		c.Store.Region = d.Store.Region
	}
	if c.Store.GC.Interval == 0 {
		c.Store.GC.Interval = d.Store.GC.Interval
	}
	if c.Store.GC.Grace == 0 {
		c.Store.GC.Grace = d.Store.GC.Grace
	}
	if c.Store.Replica.Mode == "" {
		c.Store.Replica.Mode = ReplicaOff
	}
	for i := range c.Agent.Sites {
		s := &c.Agent.Sites[i]
		if s.Backend.Region == "" {
			s.Backend.Region = c.Store.Region
		}
		if s.Backend.Ack == "" {
			// Durability over latency: an acknowledged write should
			// mean the bytes are somewhere that survives this machine.
			s.Backend.Ack = AckRemote
		}
		if s.Cache.KeepBelow == 0 {
			s.Cache.KeepBelow = 64 << 10
		}
		if s.Cache.Interval == 0 {
			s.Cache.Interval = Duration(time.Minute)
		}
	}
}

func (c *Config) warnf(format string, a ...any) {
	c.Warnings = append(c.Warnings, fmt.Sprintf(format, a...))
}

// validate checks the effective configuration, dropping unusable list
// entries with a warning and returning an error only for problems that
// make the daemon pointless to start.
func (c *Config) validate() error {
	if len(c.Roles) == 0 {
		return fmt.Errorf("roles is empty: declare at least one of [store, agent]")
	}
	for _, r := range c.Roles {
		if r != RoleStore && r != RoleAgent {
			return fmt.Errorf("unknown role %q (want store or agent)", r)
		}
	}
	if c.Has(RoleStore) {
		if err := c.validateStore(); err != nil {
			return err
		}
	}
	if c.Has(RoleAgent) {
		if err := c.validateAgent(); err != nil {
			return err
		}
	}
	return nil
}

func (c *Config) validateStore() error {
	s := &c.Store
	if !isAbs(s.Data) {
		return fmt.Errorf("store.data %q must be an absolute path", s.Data)
	}
	if _, _, err := net.SplitHostPort(s.Listen); err != nil {
		return fmt.Errorf("store.listen %q: %w", s.Listen, err)
	}

	seen := make(map[string]bool, len(s.Buckets))
	kept := s.Buckets[:0]
	for _, b := range s.Buckets {
		switch {
		case b.Name == "":
			c.warnf("store: bucket without a name, skipped")
		case !validBucketName(b.Name):
			c.warnf("store: bucket %q has an invalid name, skipped", b.Name)
		case seen[b.Name]:
			c.warnf("store: bucket %q declared twice, later entry skipped", b.Name)
		case b.AccessKey == "" || b.SecretKey == "":
			c.warnf("store: bucket %q has no credentials, skipped", b.Name)
		default:
			seen[b.Name] = true
			kept = append(kept, b)
			continue
		}
	}
	s.Buckets = kept
	if len(s.Buckets) == 0 {
		return fmt.Errorf("store role declared but no usable bucket configured")
	}

	switch s.Replica.Mode {
	case ReplicaOff:
	case ReplicaPrimary:
		if len(s.Replica.Peers) == 0 {
			c.warnf("store: replica.mode is primary but no peers listed, nothing will be shipped")
		}
	case ReplicaSecondary:
		if s.Replica.From == "" {
			return fmt.Errorf("store.replica.mode is secondary but replica.from is empty")
		}
	default:
		return fmt.Errorf("unknown store.replica.mode %q (want off, primary or secondary)", s.Replica.Mode)
	}
	return nil
}

func (c *Config) validateAgent() error {
	a := &c.Agent
	seenName := make(map[string]bool, len(a.Sites))
	seenPath := make(map[string]string, len(a.Sites))

	kept := a.Sites[:0]
	for _, s := range a.Sites {
		if err := c.checkSite(s, seenName, seenPath); err != nil {
			c.warnf("agent: site %s skipped: %v", siteLabel(s), err)
			continue
		}
		seenName[s.Name] = true
		seenPath[path.Clean(s.Uploads)] = s.Name
		seenPath[path.Clean(s.Backing)] = s.Name
		kept = append(kept, s)
	}
	a.Sites = kept
	if len(a.Sites) == 0 {
		return fmt.Errorf("agent role declared but no usable site configured")
	}
	return nil
}

// checkSite returns why a site is unusable, or nil when it is fine.
func (c *Config) checkSite(s Site, seenName map[string]bool, seenPath map[string]string) error {
	if s.Name == "" {
		return fmt.Errorf("name is empty")
	}
	if seenName[s.Name] {
		return fmt.Errorf("name declared twice")
	}
	if !isAbs(s.Uploads) {
		return fmt.Errorf("uploads %q must be an absolute path", s.Uploads)
	}
	if !isAbs(s.Backing) {
		return fmt.Errorf("backing %q must be an absolute path", s.Backing)
	}

	up, back := path.Clean(s.Uploads), path.Clean(s.Backing)
	if up == back {
		return fmt.Errorf("uploads and backing are the same path")
	}
	// A backing store inside the mountpoint (or vice versa) recurses
	// through the FUSE layer the moment the mount comes up.
	if under(back, up) || under(up, back) {
		return fmt.Errorf("uploads and backing must not be nested (%s / %s)", up, back)
	}
	if other, ok := seenPath[up]; ok {
		return fmt.Errorf("uploads path already used by site %q", other)
	}
	if other, ok := seenPath[back]; ok {
		return fmt.Errorf("backing path already used by site %q", other)
	}

	if s.Backend.Endpoint == "" {
		return fmt.Errorf("backend.endpoint is empty")
	}
	u, err := url.Parse(s.Backend.Endpoint)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return fmt.Errorf("backend.endpoint %q must be an http(s) URL", s.Backend.Endpoint)
	}
	if s.Backend.Bucket == "" {
		return fmt.Errorf("backend.bucket is empty")
	}
	if !validBucketName(s.Backend.Bucket) {
		return fmt.Errorf("backend.bucket %q is not a valid bucket name", s.Backend.Bucket)
	}
	if s.Backend.AccessKey == "" || s.Backend.SecretKey == "" {
		return fmt.Errorf("backend credentials are empty")
	}
	if s.Backend.Ack != AckRemote && s.Backend.Ack != AckLocal {
		return fmt.Errorf("backend.ack %q must be remote or local", s.Backend.Ack)
	}

	if s.Cache.MaxSize < 0 || s.Cache.MinFree < 0 || s.Cache.KeepBelow < 0 {
		return fmt.Errorf("cache sizes must not be negative")
	}
	if s.Cache.MaxSize > 0 && s.Cache.KeepBelow > s.Cache.MaxSize {
		return fmt.Errorf("cache.keep_below (%s) exceeds cache.max_size (%s)", s.Cache.KeepBelow, s.Cache.MaxSize)
	}
	return nil
}

// siteLabel names a site in a warning even when its name is missing.
func siteLabel(s Site) string {
	if s.Name != "" {
		return strconv.Quote(s.Name)
	}
	if s.Uploads != "" {
		return strconv.Quote(s.Uploads)
	}
	return "(unnamed)"
}

// isAbs reports whether p is an absolute POSIX path.
//
// Deliberately not filepath.IsAbs: a config file describes a Linux
// host whatever the host reading it happens to be, so --check-config
// must give the same answer on a workstation as it does in production.
func isAbs(p string) bool { return strings.HasPrefix(p, "/") }

// under reports whether child is inside parent. Both must already be
// cleaned.
func under(child, parent string) bool {
	if parent == "/" {
		return true
	}
	return child == parent || strings.HasPrefix(child, parent+"/")
}

// validBucketName applies the S3 naming rules that matter here:
// lowercase alphanumerics, dashes and dots, 3-63 chars, no leading or
// trailing separator. Stricter than S3 in that IP-shaped names are not
// rejected — nothing in tana resolves a bucket name as a host.
func validBucketName(name string) bool {
	if len(name) < 3 || len(name) > 63 {
		return false
	}
	if name[0] == '-' || name[0] == '.' || name[len(name)-1] == '-' || name[len(name)-1] == '.' {
		return false
	}
	for i := 0; i < len(name); i++ {
		ch := name[i]
		switch {
		case ch >= 'a' && ch <= 'z', ch >= '0' && ch <= '9', ch == '-', ch == '.':
		default:
			return false
		}
	}
	return !strings.Contains(name, "..")
}
