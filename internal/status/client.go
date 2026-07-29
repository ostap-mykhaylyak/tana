package status

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"
)

// Run implements --status / --status-json / --watch. It returns the
// process exit code: 0 when the daemon answered, 1 otherwise, so a
// health check can just test the status.
func Run(version, socket, cfgPath string, jsonOut bool, watch time.Duration) int {
	if watch <= 0 {
		return printOnce(version, socket, cfgPath, jsonOut, os.Stdout)
	}
	for {
		// Clear the screen and home the cursor, top(1) style.
		fmt.Print("\033[H\033[2J")
		printOnce(version, socket, cfgPath, jsonOut, os.Stdout)
		time.Sleep(watch)
	}
}

func printOnce(version, socket, cfgPath string, jsonOut bool, out io.Writer) int {
	info, err := Query(socket)
	if err != nil {
		if jsonOut {
			json.NewEncoder(out).Encode(map[string]any{
				"running": false,
				"version": version,
				"config":  cfgPath,
				"error":   err.Error(),
			})
		} else {
			fmt.Fprintf(out, "tana %s: not running (%v)\n", version, err)
			fmt.Fprintf(out, "config: %s\n", cfgPath)
			fmt.Fprintln(out, "\nstart it with: tana start")
		}
		return 1
	}
	if jsonOut {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		enc.Encode(info)
		return 0
	}
	render(info, out)
	return 0
}

// Query opens the control socket and asks for the current status.
func Query(socket string) (Info, error) {
	conn, err := net.DialTimeout("unix", socket, 2*time.Second)
	if err != nil {
		return Info{}, fmt.Errorf("control socket unavailable")
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))

	if _, err := io.WriteString(conn, CmdStatus+"\n"); err != nil {
		return Info{}, err
	}
	var info Info
	if err := json.NewDecoder(conn).Decode(&info); err != nil {
		return Info{}, fmt.Errorf("malformed reply: %w", err)
	}
	return info, nil
}

func render(i Info, out io.Writer) {
	fmt.Fprintf(out, "tana %s  running  pid %d  up %s\n", i.Version, i.PID, i.Uptime)
	fmt.Fprintf(out, "roles: %s\n", strings.Join(i.Roles, ", "))
	fmt.Fprintf(out, "config: %s\n", i.Config)
	if i.ConfigError != "" {
		fmt.Fprintf(out, "  reload failed, still serving the last good config: %s\n", i.ConfigError)
	}
	for _, w := range i.Warnings {
		fmt.Fprintf(out, "  warning: %s\n", w)
	}

	if s := i.Store; s != nil {
		fmt.Fprintf(out, "\nstore  listen %s  data %s  replica %s\n", s.Listen, s.Data, s.Replica)
		lag := ""
		if s.AppliedSeq < s.JournalSeq {
			lag = fmt.Sprintf("  (index behind by %d)", s.JournalSeq-s.AppliedSeq)
		}
		fmt.Fprintf(out, "  journal seq %d, applied %d%s\n", s.JournalSeq, s.AppliedSeq, lag)
		if s.JournalNote != "" {
			fmt.Fprintf(out, "  %s\n", s.JournalNote)
		}
		renderNamespaces(out, "bucket", s.Buckets)
	}
	if a := i.Agent; a != nil {
		fmt.Fprintln(out, "\nagent")
		renderNamespaces(out, "site", a.Sites)
	}

	if len(i.Caps) > 0 {
		fmt.Fprintln(out, "\nhost")
		for _, l := range i.Caps {
			fmt.Fprintf(out, "  %s\n", l)
		}
	}
}

func renderNamespaces(out io.Writer, kind string, list []Namespace) {
	if len(list) == 0 {
		fmt.Fprintf(out, "  no %s configured\n", kind)
		return
	}
	for _, n := range list {
		st := n.Stats
		fmt.Fprintf(out, "  %-28s %8d obj  %10s total  %10s local  %d dirty\n",
			n.Name, st.Objects, humanBytes(st.Bytes), humanBytes(st.LocalBytes), st.DirtyObjects)
		if n.Note != "" {
			fmt.Fprintf(out, "  %-28s %s\n", "", n.Note)
		}
	}
}

// humanBytes renders a byte count in the largest unit that keeps it
// under four digits.
func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for v := n / unit; v >= unit && exp < 4; v /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTP"[exp])
}
