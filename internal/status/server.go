package status

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Server is the listening control socket.
type Server struct {
	ln   net.Listener
	path string
}

// Serve starts the control socket at path. A stale socket left behind
// by a crash is removed first: the alternative is a daemon that starts
// fine but is invisible to --status forever.
func Serve(path string, collect Collector) (*Server, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("control socket: %w", err)
	}
	if err := removeStale(path); err != nil {
		return nil, err
	}
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, fmt.Errorf("control socket: %w", err)
	}
	// The socket exposes configuration details; keep it to root and the
	// service group.
	if err := os.Chmod(path, 0o660); err != nil {
		ln.Close()
		return nil, fmt.Errorf("control socket: %w", err)
	}
	s := &Server{ln: ln, path: path}
	go s.accept(collect)
	return s, nil
}

// removeStale unlinks a socket file nobody is listening on.
func removeStale(path string) error {
	if _, err := os.Stat(path); err != nil {
		return nil
	}
	if c, err := net.DialTimeout("unix", path, 200*time.Millisecond); err == nil {
		c.Close()
		return fmt.Errorf("control socket %s is already in use: another tana is running", path)
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("remove stale socket %s: %w", path, err)
	}
	return nil
}

func (s *Server) accept(collect Collector) {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return // listener closed
		}
		go handle(conn, collect)
	}
}

func handle(conn net.Conn, collect Collector) {
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(10 * time.Second))

	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil && line == "" {
		return
	}
	enc := json.NewEncoder(conn)
	switch strings.TrimSpace(line) {
	case CmdStatus, "":
		enc.Encode(collect())
	default:
		enc.Encode(map[string]string{"error": "unknown command"})
	}
}

// Close stops listening and removes the socket file.
func (s *Server) Close() error {
	err := s.ln.Close()
	os.Remove(s.path)
	return err
}
