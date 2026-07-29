// tana - private S3 object store and WordPress uploads agent.
//
// The CLI has two shapes and they never overlap. Service verbs
// (start, stop, restart, reload, enable, disable) carry no dashes and
// act on the systemd unit. Everything else is a --flag: lifecycle
// flags (--init, --purge) act on the filesystem from the standalone
// binary, client flags (--status, --watch) query the RUNNING daemon
// through its local Unix socket, and the rest are one-shot helpers.
//
// With no arguments at all the binary IS the daemon, which is what the
// systemd unit runs.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"os/user"
	"strconv"
	"syscall"
	"time"

	"github.com/ostap-mykhaylyak/tana/internal/agent"
	"github.com/ostap-mykhaylyak/tana/internal/bootstrap"
	"github.com/ostap-mykhaylyak/tana/internal/caps"
	"github.com/ostap-mykhaylyak/tana/internal/config"
	"github.com/ostap-mykhaylyak/tana/internal/index"
	"github.com/ostap-mykhaylyak/tana/internal/logging"
	"github.com/ostap-mykhaylyak/tana/internal/mount"
	"github.com/ostap-mykhaylyak/tana/internal/paths"
	"github.com/ostap-mykhaylyak/tana/internal/s3"
	"github.com/ostap-mykhaylyak/tana/internal/service"
	"github.com/ostap-mykhaylyak/tana/internal/status"
	"github.com/ostap-mykhaylyak/tana/internal/store"
)

// version is injected at build time via -ldflags "-X main.version=...".
var version = "dev"

func main() {
	// Service verbs are matched before flag parsing: they are a
	// different command shape, not a positional argument.
	if len(os.Args) > 1 && service.IsVerb(os.Args[1]) {
		fatalIf(service.Run(os.Args[1], os.Stdout))
		return
	}

	// --- lifecycle: act on the filesystem, standalone binary ---
	initOnly := flag.Bool("init", false, "create the filesystem layout and install the service, then exit")
	purge := flag.Bool("purge", false, "remove config, index and logs (never the blobs), then exit")
	assumeYes := flag.Bool("yes", false, "skip the confirmation prompt for --purge")

	// --- client: talk to the running daemon via its local socket ---
	statusFlag := flag.Bool("status", false, "query the running service, print status, exit")
	statusJSON := flag.Bool("status-json", false, "machine-readable status (implies --status)")
	watch := flag.Duration("watch", 0, "refresh --status every interval (e.g. 2s), like top")

	// --- one-shot helpers, no daemon and no root needed ---
	checkConfig := flag.Bool("check-config", false, "validate the config file and exit")
	showCaps := flag.Bool("caps", false, "report what this host can do, and exit")
	keygen := flag.Bool("keygen", false, "print a fresh access key / secret key pair, and exit")

	// --- offline maintenance: the service must be stopped ---
	fsck := flag.Bool("fsck", false, "check the index against the journal and the blobs, then exit")
	rebuild := flag.Bool("rebuild", false, "with --fsck, discard the index and replay the journal")
	scrub := flag.Bool("scrub", false, "verify every blob against its content hash, then exit")

	showVersion := flag.Bool("version", false, "print version and exit")
	cfgPath := flag.String("config", paths.ConfigFile, "config file")
	flag.Usage = usage
	flag.Parse()

	switch {
	case *showVersion:
		fmt.Println("tana", version)
		return
	case *keygen:
		fatalIf(bootstrap.Keygen(os.Stdout))
		return
	case *showCaps:
		printCaps(caps.Probe())
		return
	case *checkConfig:
		os.Exit(runCheckConfig(*cfgPath))
	case *initOnly:
		fatalIf(bootstrap.Init(version, os.Stdout))
		return
	case *purge:
		fatalIf(bootstrap.Purge(*assumeYes, os.Stdin, os.Stdout))
		return
	case *statusFlag || *statusJSON || *watch > 0:
		os.Exit(status.Run(version, paths.Socket, *cfgPath, *statusJSON, *watch))
	case *fsck:
		os.Exit(runFsck(*cfgPath, *rebuild))
	case *scrub:
		os.Exit(runScrub(*cfgPath))
	}

	if args := flag.Args(); len(args) > 0 {
		fmt.Fprintf(os.Stderr, "tana: unknown command %q\n\n", args[0])
		usage()
		os.Exit(2)
	}

	fatalIf(runDaemon(*cfgPath))
}

func usage() {
	fmt.Fprintf(os.Stderr, `tana %s - private S3 object store and WordPress uploads agent

usage:
  tana                    run the daemon in the foreground
  tana <%s>
  tana --<flag>

flags:
`, version, service.Names())
	flag.PrintDefaults()
}

// runCheckConfig validates the config without starting anything and
// without needing root, so it is usable from an editor or a CI job.
func runCheckConfig(cfgPath string) int {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "tana:", err)
		return 1
	}
	fmt.Printf("%s: ok\n", cfgPath)
	fmt.Printf("roles: %v\n", cfg.Roles)
	if cfg.Has(config.RoleStore) {
		fmt.Printf("store: listen %s, data %s, %d bucket(s), replica %s\n",
			cfg.Store.Listen, cfg.Store.Data, len(cfg.Store.Buckets), cfg.Store.Replica.Mode)
	}
	if cfg.Has(config.RoleAgent) {
		fmt.Printf("agent: %d site(s)\n", len(cfg.Agent.Sites))
		for _, s := range cfg.Agent.Sites {
			fmt.Printf("  %-28s %s -> %s (ack %s)\n", s.Name, s.Uploads, s.Backend.Endpoint, s.Backend.Ack)
		}
	}
	for _, w := range cfg.Warnings {
		fmt.Printf("warning: %s\n", w)
	}
	// Warnings mean something in the file was ignored. That is not
	// fatal, but a CI job should be able to notice it.
	if len(cfg.Warnings) > 0 {
		return 1
	}
	return 0
}

func printCaps(r caps.Report) {
	for _, l := range r.Lines() {
		fmt.Println(l)
	}
	fmt.Println()
	if err := r.AgentReady(); err != nil {
		fmt.Println("agent role: unavailable —", err)
	} else {
		fmt.Println("agent role: ready")
	}
	if err := r.StoreReady(); err != nil {
		fmt.Println("store role: unavailable —", err)
	} else {
		fmt.Println("store role: ready")
	}
}

func runDaemon(cfgPath string) (err error) {
	// First execution without a config: provision the default layout
	// from the embedded skel, warn on stderr and keep going. The
	// resulting config declares no roles, so the load below fails with
	// a message telling the operator exactly what to fill in.
	if cfgPath == paths.ConfigFile {
		if _, statErr := os.Stat(cfgPath); os.IsNotExist(statErr) {
			fmt.Fprintln(os.Stderr, "tana: no config found, provisioning default layout")
			if err := bootstrap.EnsureLayout(os.Stderr); err != nil {
				return err
			}
		}
	}

	mgr, err := config.NewManager(cfgPath)
	if err != nil {
		return err
	}
	cfg := mgr.Get()

	// Fail before opening anything if a declared role cannot run here.
	host := caps.Probe()
	if cfg.Has(config.RoleAgent) {
		if err := host.AgentReady(); err != nil {
			return fmt.Errorf("agent role declared: %w", err)
		}
	}

	logs, err := logging.Open(paths.LogDir)
	if err != nil {
		return err
	}
	defer logs.Close()
	// Surface a fatal startup error in the service log too, not only on
	// stderr — otherwise a crash loop is invisible to anyone reading
	// tana.log. Runs before logs.Close.
	defer func() {
		if err != nil {
			logs.Service.Error("fatal error, exiting", "error", err.Error())
		}
	}()

	started := time.Now()
	logs.Service.Info("starting",
		"version", version, "config", cfgPath, "pid", os.Getpid(), "roles", cfg.Roles)
	for _, w := range cfg.Warnings {
		logs.Service.Warn("config warning", "warning", w)
	}
	if cfg.Has(config.RoleAgent) && !host.Passthrough {
		logs.Service.Warn("FUSE passthrough unavailable, cached reads will cross userspace on every I/O",
			"kernel", host.Kernel)
	}

	if err := os.MkdirAll(paths.StateDir, 0o750); err != nil {
		return fmt.Errorf("create %s: %w", paths.StateDir, err)
	}
	idx, err := index.Open(paths.IndexFile)
	if err != nil {
		return err
	}
	defer idx.Close()
	logs.Service.Info("index open", "path", idx.Path())

	stop := make(chan struct{})

	// The blob store, journal and collector. The S3 API in front of
	// them lands in M2; until then the store is reachable only through
	// --status, which is enough to run it and watch it recover.
	var st *store.Store
	var api *http.Server
	var secondary *store.Secondary
	if cfg.Has(config.RoleStore) {
		st, err = store.New(cfg.Store, idx, logs.Service, logs.Transfer)
		if err != nil {
			return err
		}
		defer st.Close()
		// Replay before serving anything: an index that is behind the
		// journal must never answer a question.
		if _, err := st.Recover(); err != nil {
			return err
		}
		st.StartGC(stop)
		logs.Service.Info("store open",
			"data", cfg.Store.Data, "buckets", len(cfg.Store.Buckets),
			"journal_seq", st.LastSeq(), "gc_interval", cfg.Store.GC.Interval.Std().String())

		// The S3 API, plus the replication endpoint on a path S3 keys
		// cannot produce.
		handler := http.NewServeMux()
		handler.Handle("/", s3.New(st, cfg.Store.Region, logs.Access, logs.Service))
		if cfg.Store.Replica.Mode == config.ReplicaPrimary {
			handler.Handle("/-/replica/", st.ReplicaHandler(cfg.Store.Replica.Secret))
			logs.Service.Info("replication endpoint enabled",
				"peers", cfg.Store.Replica.Peers)
		}

		api = &http.Server{
			Addr:    cfg.Store.Listen,
			Handler: handler,
			// Media uploads can be slow on a bad link; the read timeout
			// has to allow a whole object, so only the header is bounded
			// tightly. Idle connections are cheap and CDN origins reuse
			// them heavily.
			ReadHeaderTimeout: 10 * time.Second,
			IdleTimeout:       120 * time.Second,
		}
		ln, err := net.Listen("tcp", cfg.Store.Listen)
		if err != nil {
			return fmt.Errorf("s3 listener: %w", err)
		}
		go func() {
			if err := api.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logs.Service.Error("s3 api stopped", "error", err)
			}
		}()
		logs.Service.Info("s3 api listening", "addr", cfg.Store.Listen, "region", cfg.Store.Region)

		if cfg.Store.Replica.Mode == config.ReplicaSecondary {
			secondary = store.NewSecondary(st, cfg.Store.Replica)
			secondary.Start(stop, cfg.Store.Replica.Interval.Std())
			logs.Service.Info("replicating from primary",
				"from", cfg.Store.Replica.From,
				"interval", cfg.Store.Replica.Interval.Std().String())
		}
	}
	// One agent per site: index namespace, writeback queue, watcher,
	// eviction pass and FUSE mount.
	agents := map[string]*agent.Agent{}
	var mounts []*mount.Mount
	if cfg.Has(config.RoleAgent) {
		for _, site := range cfg.Agent.Sites {
			ag, err := agent.New(site, idx, logs.Service, logs.Transfer)
			if err != nil {
				return err
			}
			if err := ag.Start(stop); err != nil {
				return fmt.Errorf("site %s: %w", site.Name, err)
			}
			ag.StartEviction(stop, site.Cache.Interval.Std())
			agents[site.Name] = ag

			uid, err := resolveUID(site.Mount.PopulateUser)
			if err != nil {
				return fmt.Errorf("site %s: %w", site.Name, err)
			}
			mnt, err := mount.New(ag, mount.Options{
				PopulateUID: uid,
				AllowOther:  site.Mount.AllowOther,
				Debug:       site.Mount.Debug,
			}, logs.Service)
			if err != nil {
				return fmt.Errorf("site %s: %w", site.Name, err)
			}
			mounts = append(mounts, mnt)

			logs.Service.Info("site started",
				"site", site.Name, "uploads", site.Uploads, "backing", site.Backing,
				"endpoint", site.Backend.Endpoint, "bucket", site.Backend.Bucket,
				"populate_uid", uid)
		}
	}

	if err := mgr.Watch(stop,
		func(err error) { logs.Service.Error("config reload failed", "error", err) },
		func(cfg *config.Config) {
			logs.Service.Info("config reloaded", "warnings", len(cfg.Warnings))
			for _, w := range cfg.Warnings {
				logs.Service.Warn("config warning", "warning", w)
			}
			// Only the tenant table and the collection schedule are
			// hot-swappable; the data root is bound at startup.
			if st != nil {
				st.Configure(cfg.Store)
			}
		}); err != nil {
		return err
	}

	// Local control socket: the IPC channel behind --status. If it
	// fails the daemon still serves; --status will report not running.
	collect := collector(version, started, mgr, idx, host, st, agents, secondary)
	statusSrv, err := status.Serve(paths.Socket, collect)
	if err != nil {
		logs.Service.Error("control socket unavailable", "error", err)
	}

	// Single signal loop: SIGHUP reopens logs (logrotate hook),
	// SIGTERM/SIGINT shut down gracefully.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	for s := range sig {
		if s == syscall.SIGHUP {
			logs.Service.Info("SIGHUP received, reopening log files")
			if err := logs.Reopen(); err != nil {
				logs.Service.Error("log reopen failed", "error", err)
			}
			continue
		}
		logs.Service.Info("shutting down", "signal", s.String())
		close(stop)
		// Unmount before anything else: a mount left behind after the
		// daemon exits makes the uploads directory unreadable, which is
		// a worse failure than the one being shut down for.
		for _, m := range mounts {
			if err := m.Unmount(); err != nil {
				logs.Service.Error("unmount failed", "error", err)
			}
		}
		if api != nil {
			// Let in-flight uploads finish: cutting one off mid-body
			// leaves an orphan blob for the collector and an error the
			// client did not deserve.
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
			api.Shutdown(ctx)
			cancel()
		}
		if statusSrv != nil {
			statusSrv.Close()
		}
		logs.Service.Info("shutdown complete")
		return nil
	}
	return nil
}

// collector builds the --status snapshot from live state. st is nil
// when this machine does not run the store role.
func collector(version string, started time.Time, mgr *config.Manager, idx *index.DB, host caps.Report, st *store.Store, agents map[string]*agent.Agent, secondary *store.Secondary) status.Collector {
	return func() status.Info {
		cfg := mgr.Get()
		roles := make([]string, 0, len(cfg.Roles))
		for _, r := range cfg.Roles {
			roles = append(roles, string(r))
		}
		info := status.Info{
			Version:     version,
			PID:         os.Getpid(),
			Started:     started,
			Uptime:      time.Since(started).Truncate(time.Second).String(),
			Config:      mgr.Path(),
			ConfigError: mgr.LastError(),
			Warnings:    cfg.Warnings,
			Roles:       roles,
			Caps:        host.Lines(),
		}
		if cfg.Has(config.RoleStore) {
			s := &status.StoreInfo{
				Listen:  cfg.Store.Listen,
				Data:    cfg.Store.Data,
				Replica: string(cfg.Store.Replica.Mode),
			}
			if secondary != nil {
				rep := secondary.Status()
				s.Replica = fmt.Sprintf("secondary of %s, lag %d", rep.From, rep.Lag)
				if rep.Error != "" {
					s.JournalNote = "replication: " + rep.Error
				}
			}
			if st != nil {
				s.JournalSeq = st.LastSeq()
				if applied, err := st.AppliedSeq(); err == nil {
					s.AppliedSeq = applied
				} else {
					s.JournalNote = "journal position unreadable: " + err.Error()
				}
			}
			for _, b := range cfg.Store.Buckets {
				s.Buckets = append(s.Buckets, namespace(idx, b.Name, "S3 API not implemented yet (M2)"))
			}
			info.Store = s
		}
		if cfg.Has(config.RoleAgent) {
			a := &status.AgentInfo{}
			for _, site := range cfg.Agent.Sites {
				note := ""
				if _, running := agents[site.Name]; !running {
					note = "not running"
				}
				a.Sites = append(a.Sites, namespace(idx, site.Name, note))
			}
			info.Agent = a
		}
		return info
	}
}

// namespace reads one namespace's counters, degrading to the note
// rather than failing the whole status call.
func namespace(idx *index.DB, name, note string) status.Namespace {
	ns := status.Namespace{Name: name, Note: note}
	if st, err := idx.Stats(name); err == nil {
		ns.Stats = st
	} else {
		ns.Note = "index unreadable: " + err.Error()
	}
	return ns
}

// offlineStore opens the store outside the daemon, for maintenance.
//
// It refuses to run against a live service. Both checks walk the same
// files the daemon has open, and a rebuild rewrites the index the
// daemon is serving from; doing that underneath a running process is
// how a repair tool becomes the outage.
func offlineStore(cfgPath string) (*store.Store, *index.DB, error) {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return nil, nil, err
	}
	if !cfg.Has(config.RoleStore) {
		return nil, nil, fmt.Errorf("this machine does not run the store role")
	}
	if err := exec.Command("systemctl", "is-active", "--quiet", paths.ServiceName).Run(); err == nil {
		return nil, nil, fmt.Errorf("tana is running: stop it first (tana stop)")
	}

	idx, err := index.Open(paths.IndexFile)
	if err != nil {
		return nil, nil, err
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	st, err := store.New(cfg.Store, idx, logger, logger)
	if err != nil {
		idx.Close()
		return nil, nil, err
	}
	return st, idx, nil
}

// runFsck implements --fsck.
func runFsck(cfgPath string, rebuild bool) int {
	st, idx, err := offlineStore(cfgPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "tana:", err)
		return 1
	}
	defer func() { st.Close(); idx.Close() }()

	rep, err := st.Fsck(rebuild)
	if err != nil {
		fmt.Fprintln(os.Stderr, "tana:", err)
		return 1
	}
	if rep.Rebuilt {
		fmt.Printf("rebuilt the index from %d journal record(s)\n", rep.JournalRecords)
	}
	fmt.Printf("objects:            %d\n", rep.Objects)
	fmt.Printf("references:         %d\n", rep.References)
	fmt.Printf("unreferenced blobs: %d (the collector removes these)\n", rep.UnreferencedBlobs)
	fmt.Printf("stale references:   %d\n", rep.StaleRefs)
	fmt.Printf("checked in %s\n", rep.Duration.Truncate(time.Millisecond))

	if len(rep.MissingBlobs) > 0 {
		fmt.Printf("\n%d object(s) reference content that is not on disk:\n", len(rep.MissingBlobs))
		for i, k := range rep.MissingBlobs {
			if i == 20 {
				fmt.Printf("  ... and %d more\n", len(rep.MissingBlobs)-20)
				break
			}
			fmt.Println("  ", k)
		}
		// Say plainly which findings are bookkeeping and which are not.
		// An operator reading a repair tool's output at three in the
		// morning should not have to work out which line is the bad one.
		fmt.Println("\nThis is data loss, not bookkeeping: restore those objects from a")
		fmt.Println("secondary or a backup. Rebuilding the index will not bring them back.")
	}
	if rep.Healthy() {
		fmt.Println("\nno problems found")
		return 0
	}
	return 1
}

// runScrub implements --scrub.
func runScrub(cfgPath string) int {
	st, idx, err := offlineStore(cfgPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "tana:", err)
		return 1
	}
	defer func() { st.Close(); idx.Close() }()

	fmt.Println("verifying every blob against its content hash; this reads the whole store")
	rep, err := st.Scrub(func(blobs, bytes int64) {
		fmt.Printf("  %d blobs, %d bytes so far\n", blobs, bytes)
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "tana:", err)
		return 1
	}
	fmt.Printf("blobs:   %d\n", rep.Blobs)
	fmt.Printf("bytes:   %d\n", rep.Bytes)
	fmt.Printf("foreign: %d (files tana did not write, left alone)\n", rep.Foreign)
	fmt.Printf("read in %s\n", rep.Duration.Truncate(time.Millisecond))

	if len(rep.Corrupt) > 0 {
		fmt.Printf("\n%d blob(s) no longer match their hash:\n", len(rep.Corrupt))
		for _, h := range rep.Corrupt {
			fmt.Println("  ", h)
		}
		fmt.Println("\nThe content rotted underneath tana. Whatever redundancy sits below")
		fmt.Println("the blob store did not do its job; check the array before restoring.")
		return 1
	}
	fmt.Println("\nevery blob matches its hash")
	return 0
}

// resolveUID turns a configured account name into a numeric uid. A
// numeric value is accepted as-is, so a deployment without the account
// in its passwd database can still name it.
func resolveUID(name string) (uint32, error) {
	if name == "" {
		return 0, nil
	}
	if n, err := strconv.ParseUint(name, 10, 32); err == nil {
		return uint32(n), nil
	}
	u, err := user.Lookup(name)
	if err != nil {
		return 0, fmt.Errorf("mount.populate_user %q: %w", name, err)
	}
	n, err := strconv.ParseUint(u.Uid, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("mount.populate_user %q: unreadable uid %q", name, u.Uid)
	}
	return uint32(n), nil
}

func fatalIf(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "tana:", err)
		os.Exit(1)
	}
}
