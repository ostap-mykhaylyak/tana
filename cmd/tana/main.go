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
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ostap-mykhaylyak/tana/internal/agent"
	"github.com/ostap-mykhaylyak/tana/internal/bootstrap"
	"github.com/ostap-mykhaylyak/tana/internal/caps"
	"github.com/ostap-mykhaylyak/tana/internal/config"
	"github.com/ostap-mykhaylyak/tana/internal/index"
	"github.com/ostap-mykhaylyak/tana/internal/logging"
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

		api = &http.Server{
			Addr:    cfg.Store.Listen,
			Handler: s3.New(st, cfg.Store.Region, logs.Access, logs.Service),
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
	}
	// One agent per site: index namespace, writeback queue and watcher.
	agents := map[string]*agent.Agent{}
	if cfg.Has(config.RoleAgent) {
		for _, site := range cfg.Agent.Sites {
			ag, err := agent.New(site, idx, logs.Service, logs.Transfer)
			if err != nil {
				return err
			}
			if err := ag.Start(stop); err != nil {
				return fmt.Errorf("site %s: %w", site.Name, err)
			}
			agents[site.Name] = ag
			logs.Service.Info("site started",
				"site", site.Name, "backing", site.Backing,
				"endpoint", site.Backend.Endpoint, "bucket", site.Backend.Bucket)
		}
		logs.Service.Warn("agent role: the FUSE mount is not implemented yet (M4); "+
			"uploads is not yet backed by tana, only the backing directory is mirrored",
			"sites", len(cfg.Agent.Sites))
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
	collect := collector(version, started, mgr, idx, host, st, agents)
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
func collector(version string, started time.Time, mgr *config.Manager, idx *index.DB, host caps.Report, st *store.Store, agents map[string]*agent.Agent) status.Collector {
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
				note := "not mounted yet (M4): mirroring the backing directory"
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

func fatalIf(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "tana:", err)
		os.Exit(1)
	}
}
