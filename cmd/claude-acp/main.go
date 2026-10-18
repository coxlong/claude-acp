// Command claude-acp serves an ACP agent over WebSocket, backed by the
// Claude Code CLI.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/coxlong/claude-acp/internal/acp"
	"github.com/coxlong/claude-acp/internal/agent"
	"github.com/coxlong/claude-acp/internal/rpc"
	"github.com/coxlong/claude-acp/internal/ws"
)

var version = "dev"

func main() {
	if len(os.Args) < 2 {
		runServe(nil)
		return
	}
	switch os.Args[1] {
	case "serve":
		runServe(os.Args[2:])
	case "version", "-v", "--version":
		fmt.Println("claude-acp", version)
	case "doctor":
		runDoctor(os.Args[2:])
	default:
		if len(os.Args[1]) > 0 && os.Args[1][0] == '-' {
			runServe(os.Args[1:])
			return
		}
		fmt.Fprintf(os.Stderr, "usage: claude-acp [serve|version|doctor] [flags]\n")
		os.Exit(2)
	}
}

func runServe(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	addr := fs.String("addr", envOr("CLAUDE_ACP_ADDR", "127.0.0.1:3000"), "listen address")
	token := fs.String("token", os.Getenv("CLAUDE_ACP_TOKEN"), "bearer token required for non-loopback binds")
	claudePath := fs.String("claude-path", os.Getenv("CLAUDE_CODE_EXECUTABLE"), "path to the claude CLI")
	cwdRoot := fs.String("cwd-root", os.Getenv("CLAUDE_ACP_CWD_ROOT"), "optional allowlist root for session cwd")
	sessionGrace := fs.Duration("session-grace", 10*time.Minute, "how long to keep sessions after a disconnect")
	outboxMax := fs.Int("outbox-max", 8<<20, "outbox byte limit per client")
	maxSessions := fs.Int("max-sessions", 16, "maximum sessions per client")
	logLevel := fs.String("log-level", "info", "log level: debug, info, warn, error")
	tlsCert := fs.String("tls-cert", "", "TLS certificate file")
	tlsKey := fs.String("tls-key", "", "TLS key file")
	configDir := fs.String("config-dir", "", "Claude config directory (default ~/.claude)")
	strictClientID := fs.Bool("strict-client-id", false, "require the slot secret (from the connection preamble) on reconnect with X-Client-Id")
	_ = fs.Parse(args)

	logger := newLogger(*logLevel)

	server, err := ws.NewServer(ws.Config{
		Addr:           *addr,
		Token:          *token,
		TLSCert:        *tlsCert,
		TLSKey:         *tlsKey,
		SessionGrace:   *sessionGrace,
		OutboxMax:      *outboxMax,
		StrictClientID: *strictClientID,
		Log:            logger,
	}, func(conn *rpc.Conn) rpc.Handler {
		a := agent.New(conn, agent.Config{
			ClaudePath:  *claudePath,
			CwdRoot:     *cwdRoot,
			MaxSessions: *maxSessions,
			Log:         logger,
			ConfigDir:   *configDir,
		})
		return acp.NewServer(a)
	})
	if err != nil {
		logger.Error("failed to start", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := server.Run(ctx); err != nil {
		logger.Error("server exited", "err", err)
		os.Exit(1)
	}
}

func runDoctor(args []string) {
	fs := flag.NewFlagSet("doctor", flag.ExitOnError)
	claudePath := fs.String("claude-path", os.Getenv("CLAUDE_CODE_EXECUTABLE"), "path to the claude CLI")
	_ = fs.Parse(args)

	path := *claudePath
	if path == "" {
		path = "claude"
	}
	resolved, err := exec.LookPath(path)
	if err != nil {
		fmt.Printf("claude: not found (%v)\n", err)
		os.Exit(1)
	}
	fmt.Println("claude:", resolved)

	out, err := exec.Command(resolved, "--version").Output()
	if err != nil {
		fmt.Printf("version check failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("version:", string(trimNewline(out)))

	configDir := os.Getenv("CLAUDE_CONFIG_DIR")
	if configDir == "" {
		if home, err := os.UserHomeDir(); err == nil {
			configDir = filepath.Join(home, ".claude")
		}
	}
	if _, err := os.Stat(configDir); err != nil {
		fmt.Println("config dir: missing", configDir)
	} else {
		fmt.Println("config dir:", configDir)
	}
}

func newLogger(level string) *slog.Logger {
	var lv slog.Level
	switch level {
	case "debug":
		lv = slog.LevelDebug
	case "warn":
		lv = slog.LevelWarn
	case "error":
		lv = slog.LevelError
	default:
		lv = slog.LevelInfo
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lv}))
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func trimNewline(b []byte) []byte {
	for len(b) > 0 && (b[len(b)-1] == '\n' || b[len(b)-1] == '\r') {
		b = b[:len(b)-1]
	}
	return b
}
