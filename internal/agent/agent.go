// Package agent bridges ACP sessions to Claude Code CLI processes.
package agent

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/coxlong/claude-acp/internal/acp"
	"github.com/coxlong/claude-acp/internal/claude"
	"github.com/coxlong/claude-acp/internal/rpc"
)

// Config controls the agent.
type Config struct {
	ClaudePath  string
	CwdRoot     string
	MaxSessions int
	Env         map[string]string
	Log         *slog.Logger
	// ConfigDir overrides the Claude config directory (default ~/.claude or
	// $CLAUDE_CONFIG_DIR).
	ConfigDir string
}

// Agent implements acp.Handler.
type Agent struct {
	conn *rpc.Conn
	cfg  Config
	log  *slog.Logger

	mu       sync.Mutex
	sessions map[string]*Session
	closed   bool
}

// New creates an agent bound to one logical ACP connection.
func New(conn *rpc.Conn, cfg Config) *Agent {
	if cfg.MaxSessions <= 0 {
		cfg.MaxSessions = 16
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	return &Agent{
		conn:     conn,
		cfg:      cfg,
		log:      cfg.Log,
		sessions: make(map[string]*Session),
	}
}

// Close terminates all sessions. It implements io.Closer.
func (a *Agent) Close() error {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return nil
	}
	a.closed = true
	sessions := make([]*Session, 0, len(a.sessions))
	for _, s := range a.sessions {
		sessions = append(sessions, s)
	}
	a.sessions = make(map[string]*Session)
	a.mu.Unlock()

	for _, s := range sessions {
		_ = s.client.Close()
	}
	return nil
}

// Initialize implements acp.Handler.
func (a *Agent) Initialize(_ context.Context, _ acp.InitializeRequest) (acp.InitializeResponse, *rpc.Error) {
	return acp.InitializeResponse{
		ProtocolVersion: acp.ProtocolVersion,
		AgentCapabilities: acp.AgentCapabilities{
			LoadSession:         true,
			PromptCapabilities:  acp.PromptCapabilities{},
			McpCapabilities:     acp.McpCapabilities{HTTP: true, SSE: true},
			SessionCapabilities: &acp.SessionCapabilities{List: &acp.SessionListCapabilities{}, Close: &acp.SessionCloseCapabilities{}},
		},
		AuthMethods: []acp.AuthMethod{},
	}, nil
}

// Authenticate implements acp.Handler. Authentication is handled by the CLI's
// local credentials, so this is a no-op.
func (a *Agent) Authenticate(context.Context, acp.AuthenticateRequest) (acp.AuthenticateResponse, *rpc.Error) {
	return acp.AuthenticateResponse{}, nil
}

// NewSession implements acp.Handler.
func (a *Agent) NewSession(ctx context.Context, req acp.NewSessionRequest) (acp.NewSessionResponse, *rpc.Error) {
	if errResp := a.validateCwd(req.Cwd); errResp != nil {
		return acp.NewSessionResponse{}, errResp
	}
	if errResp := a.checkCapacity(); errResp != nil {
		return acp.NewSessionResponse{}, errResp
	}

	id := newUUID()
	sess, err := a.startSession(ctx, id, req.Cwd, "", req.McpServers, req.AdditionalDirectories)
	if err != nil {
		return acp.NewSessionResponse{}, rpc.ErrInternalMsg(err.Error())
	}
	return acp.NewSessionResponse{
		SessionID:     id,
		Modes:         sess.modes(),
		ConfigOptions: sess.snapshotConfigOptions(),
	}, nil
}

// LoadSession implements acp.Handler.
func (a *Agent) LoadSession(ctx context.Context, req acp.LoadSessionRequest) (acp.LoadSessionResponse, *rpc.Error) {
	if req.SessionID == "" {
		return acp.LoadSessionResponse{}, rpc.ErrInvalidParams("missing sessionId")
	}
	if errResp := a.validateCwd(req.Cwd); errResp != nil {
		return acp.LoadSessionResponse{}, errResp
	}
	if existing := a.getSession(req.SessionID); existing != nil {
		return acp.LoadSessionResponse{
			Modes:         existing.modes(),
			ConfigOptions: existing.snapshotConfigOptions(),
		}, nil
	}
	if errResp := a.checkCapacity(); errResp != nil {
		return acp.LoadSessionResponse{}, errResp
	}

	sess, err := a.startSession(ctx, req.SessionID, req.Cwd, req.SessionID, req.McpServers, nil)
	if err != nil {
		return acp.LoadSessionResponse{}, rpc.ErrInternalMsg(err.Error())
	}
	a.replayHistory(sess)
	return acp.LoadSessionResponse{
		Modes:         sess.modes(),
		ConfigOptions: sess.snapshotConfigOptions(),
	}, nil
}

func (a *Agent) startSession(ctx context.Context, id, cwd, resume string, servers []acp.McpServer, additionalDirs []string) (*Session, error) {
	sess := &Session{
		id:        id,
		cwd:       cwd,
		agent:     a,
		mode:      "default",
		toolCalls: make(map[string]bool),
	}

	opts := claude.Options{
		SessionID:      "",
		Resume:         resume,
		Cwd:            cwd,
		PermissionMode: sess.mode,
		McpConfig:      mcpConfigArg(servers),
		AdditionalDirs: additionalDirs,
		CliPath:        a.cfg.ClaudePath,
		Env:            a.cfg.Env,
		Log:            a.log,
		OnStderr: func(line string) {
			a.log.Debug("claude stderr", "sessionId", id, "line", line)
		},
	}
	if resume == "" {
		opts.SessionID = id
	}
	opts.CanUseTool = func(req *claude.PermissionRequest) (claude.PermissionResult, error) {
		return a.requestPermission(sess, req)
	}

	client, err := claude.Start(ctx, opts)
	if err != nil {
		return nil, err
	}
	sess.client = client
	sess.models = client.Models()
	sess.buildConfigOptions()

	a.mu.Lock()
	if a.closed || len(a.sessions) >= a.cfg.MaxSessions {
		a.mu.Unlock()
		_ = client.Close()
		return nil, fmt.Errorf("session limit reached")
	}
	a.sessions[id] = sess
	a.mu.Unlock()
	return sess, nil
}

// ListSessions implements acp.Handler.
func (a *Agent) ListSessions(_ context.Context, req acp.ListSessionsRequest) (acp.ListSessionsResponse, *rpc.Error) {
	infos, err := a.listSessions(req.Cwd)
	if err != nil {
		return acp.ListSessionsResponse{}, rpc.ErrInternalMsg(err.Error())
	}
	if infos == nil {
		infos = []acp.SessionInfo{}
	}
	return acp.ListSessionsResponse{Sessions: infos}, nil
}

// CloseSession implements acp.Handler.
func (a *Agent) CloseSession(_ context.Context, req acp.CloseSessionRequest) (acp.CloseSessionResponse, *rpc.Error) {
	a.mu.Lock()
	sess := a.sessions[req.SessionID]
	delete(a.sessions, req.SessionID)
	a.mu.Unlock()
	if sess == nil {
		return acp.CloseSessionResponse{}, nil
	}
	_ = sess.client.Close()
	return acp.CloseSessionResponse{}, nil
}

// Cancel implements acp.Handler.
func (a *Agent) Cancel(ctx context.Context, req acp.CancelNotification) *rpc.Error {
	sess := a.getSession(req.SessionID)
	if sess == nil {
		return nil
	}
	sess.markCancelled()
	callCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := sess.client.Interrupt(callCtx); err != nil {
		a.log.Debug("interrupt failed", "sessionId", req.SessionID, "err", err)
	}
	return nil
}

// SetSessionMode implements acp.Handler.
func (a *Agent) SetSessionMode(ctx context.Context, req acp.SetSessionModeRequest) (acp.SetSessionModeResponse, *rpc.Error) {
	sess := a.getSession(req.SessionID)
	if sess == nil {
		return acp.SetSessionModeResponse{}, rpc.ErrInvalidParams("unknown session: " + req.SessionID)
	}
	if !validMode(req.ModeID) {
		return acp.SetSessionModeResponse{}, rpc.ErrInvalidParams("unknown mode: " + req.ModeID)
	}
	if err := sess.client.SetPermissionMode(ctx, req.ModeID); err != nil {
		return acp.SetSessionModeResponse{}, rpc.ErrInternalMsg(err.Error())
	}
	sess.setMode(req.ModeID)
	a.notify(sess, acp.SessionUpdate{SessionUpdate: acp.UpdateCurrentMode, CurrentModeID: req.ModeID})
	a.notify(sess, acp.SessionUpdate{SessionUpdate: acp.UpdateConfigOptions, ConfigOptions: sess.snapshotConfigOptions()})
	return acp.SetSessionModeResponse{}, nil
}

// SetSessionConfigOption implements acp.Handler.
func (a *Agent) SetSessionConfigOption(ctx context.Context, req acp.SetSessionConfigOptionRequest) (acp.SetSessionConfigOptionResponse, *rpc.Error) {
	sess := a.getSession(req.SessionID)
	if sess == nil {
		return acp.SetSessionConfigOptionResponse{}, rpc.ErrInvalidParams("unknown session: " + req.SessionID)
	}
	value, _ := req.Value.(string)

	switch req.ConfigID {
	case "mode":
		if !validMode(value) {
			return acp.SetSessionConfigOptionResponse{}, rpc.ErrInvalidParams("unknown mode: " + value)
		}
		if err := sess.client.SetPermissionMode(ctx, value); err != nil {
			return acp.SetSessionConfigOptionResponse{}, rpc.ErrInternalMsg(err.Error())
		}
		sess.setMode(value)
		a.notify(sess, acp.SessionUpdate{SessionUpdate: acp.UpdateCurrentMode, CurrentModeID: value})

	case "model":
		if value == "" {
			return acp.SetSessionConfigOptionResponse{}, rpc.ErrInvalidParams("missing model value")
		}
		if err := sess.client.SetModel(ctx, value); err != nil {
			return acp.SetSessionConfigOptionResponse{}, rpc.ErrInternalMsg(err.Error())
		}
		sess.setModel(value)

	case "effort":
		if value == "" {
			return acp.SetSessionConfigOptionResponse{}, rpc.ErrInvalidParams("missing effort value")
		}
		if err := sess.client.SetEffort(ctx, value); err != nil {
			return acp.SetSessionConfigOptionResponse{}, rpc.ErrInternalMsg(err.Error())
		}
		sess.setEffort(value)

	default:
		return acp.SetSessionConfigOptionResponse{}, rpc.ErrInvalidParams("unknown config option: " + req.ConfigID)
	}

	options := sess.snapshotConfigOptions()
	a.notify(sess, acp.SessionUpdate{SessionUpdate: acp.UpdateConfigOptions, ConfigOptions: options})
	return acp.SetSessionConfigOptionResponse{ConfigOptions: options}, nil
}

func (a *Agent) getSession(id string) *Session {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.sessions[id]
}

func (a *Agent) checkCapacity() *rpc.Error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return rpc.ErrClosed
	}
	if len(a.sessions) >= a.cfg.MaxSessions {
		return rpc.ErrInvalidParams("session limit reached")
	}
	return nil
}

func (a *Agent) validateCwd(cwd string) *rpc.Error {
	if cwd == "" || !filepath.IsAbs(cwd) {
		return rpc.ErrInvalidParams("cwd must be an absolute path")
	}
	info, err := os.Stat(cwd)
	if err != nil || !info.IsDir() {
		return rpc.ErrInvalidParams("cwd is not a directory: " + cwd)
	}
	if a.cfg.CwdRoot != "" {
		root := filepath.Clean(a.cfg.CwdRoot)
		clean := filepath.Clean(cwd)
		if clean != root && !strings.HasPrefix(clean+string(os.PathSeparator), root+string(os.PathSeparator)) {
			return rpc.ErrInvalidParams("cwd is outside the allowed root")
		}
	}
	return nil
}

func (a *Agent) notify(sess *Session, update acp.SessionUpdate) {
	err := a.conn.Notify(context.Background(), acp.ClientSessionUpdate, acp.SessionNotification{
		SessionID: sess.id,
		Update:    update,
	})
	if err != nil {
		a.log.Debug("session/update failed", "sessionId", sess.id, "err", err)
	}
}

func validMode(mode string) bool {
	switch mode {
	case "default", "acceptEdits", "plan", "bypassPermissions":
		return true
	}
	return false
}

func mcpConfigArg(servers []acp.McpServer) string {
	if len(servers) == 0 {
		return ""
	}
	cfg := make(map[string]any, len(servers))
	for _, s := range servers {
		if s.Type == "http" || s.Type == "sse" {
			entry := map[string]any{"type": s.Type, "url": s.URL}
			if len(s.Headers) > 0 {
				entry["headers"] = s.Headers
			}
			cfg[s.Name] = entry
			continue
		}
		entry := map[string]any{"type": "stdio", "command": s.Command}
		if len(s.Args) > 0 {
			entry["args"] = s.Args
		}
		if len(s.Env) > 0 {
			entry["env"] = s.Env
		}
		cfg[s.Name] = entry
	}
	b, err := json.Marshal(map[string]any{"mcpServers": cfg})
	if err != nil {
		return ""
	}
	return string(b)
}

func newUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
