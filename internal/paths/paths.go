// Package paths centralizes the hardcoded FHS layout of tana.
//
// Production code relies ONLY on these constants; overrides (e.g. the
// --config flag) exist solely for testing. The blob store directory is
// deliberately NOT here: it usually lives on a dedicated volume, so it
// is configured (store.data) rather than fixed.
package paths

const (
	// Binary is where the installed executable lives.
	Binary = "/sbin/tana"

	// ConfigDir holds all configuration (read-only at runtime).
	ConfigDir  = "/etc/tana"
	ConfigFile = ConfigDir + "/config.yaml"

	// StateDir holds durable runtime state that must survive a reboot:
	// the object index and the writeback queue. Distinct from LogDir
	// because losing it is a data problem, not an observability one.
	StateDir  = "/var/lib/tana"
	IndexFile = StateDir + "/index.db"

	// LogDir holds all log files. Rotation is delegated to logrotate.
	LogDir = "/var/log/tana"

	// RunDir holds the local control socket (tmpfs, managed by systemd
	// via RuntimeDirectory=).
	RunDir = "/run/tana"
	Socket = RunDir + "/tana.sock"

	// Deploy targets used by --init.
	UnitFile      = "/etc/systemd/system/tana.service"
	LogrotateFile = "/etc/logrotate.d/tana"
	ServiceName   = "tana.service"

	// DefaultStoreData is the store.data fallback when the operator
	// leaves it empty.
	DefaultStoreData = "/srv/tana"
)

// Log file names, to be joined with LogDir.
const (
	// ServiceLog carries daemon lifecycle, config reloads and errors.
	ServiceLog = "tana.log"
	// AccessLog carries one line per S3 request served (buffered).
	AccessLog = "access.log"
	// TransferLog carries writeback uploads, recalls, evictions and
	// journal shipping to the secondaries.
	TransferLog = "transfer.log"
)
