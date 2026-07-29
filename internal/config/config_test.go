package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// write drops a config file in a temp dir and returns its path.
func write(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestParseSize(t *testing.T) {
	cases := []struct {
		in   string
		want int64
	}{
		{"0", 0},
		{"512", 512},
		{"64KB", 64 << 10},
		{"64kb", 64 << 10},
		{"64KiB", 64 << 10},
		{"20GB", 20 << 30},
		{" 1 TB ", 1 << 40},
		{"1.5GB", 1536 << 20},
		{"100B", 100},
	}
	for _, c := range cases {
		got, err := ParseSize(c.in)
		if err != nil {
			t.Errorf("ParseSize(%q): %v", c.in, err)
			continue
		}
		if int64(got) != c.want {
			t.Errorf("ParseSize(%q) = %d, want %d", c.in, got, c.want)
		}
	}
	for _, bad := range []string{"", "-5GB", "abc", "GB", "1XB"} {
		if _, err := ParseSize(bad); err == nil {
			t.Errorf("ParseSize(%q) accepted an invalid value", bad)
		}
	}
}

func TestSizeStringRoundTrip(t *testing.T) {
	for _, s := range []string{"512B", "64KB", "20GB", "1TB"} {
		v, err := ParseSize(s)
		if err != nil {
			t.Fatal(err)
		}
		if got := v.String(); got != s {
			t.Errorf("ParseSize(%q).String() = %q", s, got)
		}
	}
}

func TestLoadRequiresRoles(t *testing.T) {
	path := write(t, "roles: []\n")
	if _, err := Load(path); err == nil {
		t.Fatal("expected an error for an empty roles list")
	}
	path = write(t, "roles: [wat]\n")
	if _, err := Load(path); err == nil {
		t.Fatal("expected an error for an unknown role")
	}
}

func TestLoadStore(t *testing.T) {
	path := write(t, `
roles: [store]
store:
  data: /srv/tana
  listen: 10.0.0.1:9200
  buckets:
    - name: shop-uploads
      access_key: AK
      secret_key: SK
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Has(RoleStore) || cfg.Has(RoleAgent) {
		t.Fatalf("roles = %v", cfg.Roles)
	}
	if len(cfg.Store.Buckets) != 1 {
		t.Fatalf("buckets = %d", len(cfg.Store.Buckets))
	}
	// Defaults must be applied before validation sees the values.
	if cfg.Store.Region != "tana" || cfg.Store.GC.Grace.Std().Hours() != 72 {
		t.Errorf("defaults not applied: %+v", cfg.Store)
	}
	if cfg.Store.Replica.Mode != ReplicaOff {
		t.Errorf("replica mode = %q, want off", cfg.Store.Replica.Mode)
	}
}

func TestStoreDropsUnusableBuckets(t *testing.T) {
	path := write(t, `
roles: [store]
store:
  buckets:
    - name: good-bucket
      access_key: AK
      secret_key: SK
    - name: no-creds
    - name: UPPERCASE
      access_key: AK
      secret_key: SK
    - name: good-bucket
      access_key: AK2
      secret_key: SK2
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Store.Buckets) != 1 || cfg.Store.Buckets[0].Name != "good-bucket" {
		t.Fatalf("buckets = %+v", cfg.Store.Buckets)
	}
	if len(cfg.Warnings) != 3 {
		t.Errorf("warnings = %v, want one per skipped bucket", cfg.Warnings)
	}
}

func TestStoreWithNoUsableBucketIsFatal(t *testing.T) {
	path := write(t, `
roles: [store]
store:
  buckets:
    - name: no-creds
`)
	if _, err := Load(path); err == nil {
		t.Fatal("expected a fatal error when no bucket survives validation")
	}
}

const agentSite = `
roles: [agent]
agent:
  sites:
    - name: shop.example.com
      uploads: /var/www/shop/wp-content/uploads
      backing: /var/lib/tana/shop
      backend:
        endpoint: http://store01.lan:9200
        bucket: shop-uploads
        access_key: AK
        secret_key: SK
`

func TestLoadAgentDefaults(t *testing.T) {
	cfg, err := Load(write(t, agentSite))
	if err != nil {
		t.Fatal(err)
	}
	s := cfg.Agent.Sites[0]
	// Durability over latency unless the operator says otherwise.
	if s.Backend.Ack != AckRemote {
		t.Errorf("ack = %q, want remote", s.Backend.Ack)
	}
	if s.Backend.Region != "tana" {
		t.Errorf("region = %q, want the store default", s.Backend.Region)
	}
	if s.Cache.KeepBelow != 64<<10 {
		t.Errorf("keep_below = %s, want 64KB", s.Cache.KeepBelow)
	}
}

func TestAgentRejectsNestedPaths(t *testing.T) {
	// A backing store inside the mountpoint recurses through FUSE the
	// moment the mount comes up, so the site must be dropped.
	path := write(t, `
roles: [agent]
agent:
  sites:
    - name: shop
      uploads: /var/www/shop/wp-content/uploads
      backing: /var/www/shop/wp-content/uploads/.tana
      backend:
        endpoint: http://store01.lan:9200
        bucket: shop-uploads
        access_key: AK
        secret_key: SK
`)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected the only site to be dropped, making the agent role fatal")
	}
	if !strings.Contains(err.Error(), "no usable site") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestAgentRejectsSharedPaths(t *testing.T) {
	path := write(t, `
roles: [agent]
agent:
  sites:
    - name: a
      uploads: /var/www/a/uploads
      backing: /var/lib/tana/shared
      backend: {endpoint: "http://s:9200", bucket: bucket-a, access_key: AK, secret_key: SK}
    - name: b
      uploads: /var/www/b/uploads
      backing: /var/lib/tana/shared
      backend: {endpoint: "http://s:9200", bucket: bucket-b, access_key: AK, secret_key: SK}
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Agent.Sites) != 1 || cfg.Agent.Sites[0].Name != "a" {
		t.Fatalf("sites = %+v", cfg.Agent.Sites)
	}
	if len(cfg.Warnings) != 1 {
		t.Errorf("warnings = %v, want one for the shared backing path", cfg.Warnings)
	}
}

func TestAgentRejectsBadBackend(t *testing.T) {
	for _, backend := range []string{
		`{endpoint: "", bucket: b-ucket, access_key: AK, secret_key: SK}`,
		`{endpoint: "ftp://store/", bucket: b-ucket, access_key: AK, secret_key: SK}`,
		`{endpoint: "http://s:9200", bucket: "", access_key: AK, secret_key: SK}`,
		`{endpoint: "http://s:9200", bucket: b-ucket, access_key: "", secret_key: SK}`,
		`{endpoint: "http://s:9200", bucket: b-ucket, access_key: AK, secret_key: SK, ack: soon}`,
	} {
		body := "roles: [agent]\nagent:\n  sites:\n    - name: s\n      uploads: /a\n      backing: /b\n      backend: " + backend + "\n"
		if _, err := Load(write(t, body)); err == nil {
			t.Errorf("accepted an invalid backend: %s", backend)
		}
	}
}

func TestBothRoles(t *testing.T) {
	path := write(t, `
roles: [store, agent]
store:
  buckets:
    - {name: shop-uploads, access_key: AK, secret_key: SK}
agent:
  sites:
    - name: shop
      uploads: /var/www/shop/uploads
      backing: /var/lib/tana/shop
      backend: {endpoint: "http://127.0.0.1:9200", bucket: shop-uploads, access_key: AK, secret_key: SK}
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Has(RoleStore) || !cfg.Has(RoleAgent) {
		t.Fatalf("roles = %v", cfg.Roles)
	}
}

func TestReplicaSecondaryNeedsSource(t *testing.T) {
	path := write(t, `
roles: [store]
store:
  buckets:
    - {name: shop-uploads, access_key: AK, secret_key: SK}
  replica:
    mode: secondary
`)
	if _, err := Load(path); err == nil {
		t.Fatal("expected an error: a secondary with no primary to pull from is inert")
	}
}

func TestValidBucketName(t *testing.T) {
	for _, ok := range []string{"abc", "shop-uploads", "a.b.c", "site1"} {
		if !validBucketName(ok) {
			t.Errorf("rejected valid name %q", ok)
		}
	}
	for _, bad := range []string{"ab", "-lead", "trail-", "UPPER", "a..b", "under_score", strings.Repeat("a", 64)} {
		if validBucketName(bad) {
			t.Errorf("accepted invalid name %q", bad)
		}
	}
}
