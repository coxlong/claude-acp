// Package ws implements the WebSocket transport for ACP. Each physical
// connection is attached to a logical Slot keyed by client id; slots and
// their ACP sessions survive reconnects for a grace period.
package ws

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/coxlong/claude-acp/internal/rpc"
	"github.com/gorilla/websocket"
)

// HandlerFactory builds the ACP handler for a logical client. The handler may
// use conn for agent -> client calls and notifications. If the returned
// handler implements io.Closer, Close is invoked when the slot terminates.
type HandlerFactory func(conn *rpc.Conn) rpc.Handler

// Config controls the WebSocket server.
type Config struct {
	Addr          string
	Token         string
	TLSCert       string
	TLSKey        string
	SessionGrace  time.Duration
	OutboxMax     int
	MaxFrameBytes int64
	Log           *slog.Logger
}

func (c *Config) applyDefaults() {
	if c.Addr == "" {
		c.Addr = "127.0.0.1:3000"
	}
	if c.SessionGrace == 0 {
		c.SessionGrace = 10 * time.Minute
	}
	if c.OutboxMax == 0 {
		c.OutboxMax = 8 << 20
	}
	if c.MaxFrameBytes == 0 {
		c.MaxFrameBytes = 32 << 20
	}
	if c.Log == nil {
		c.Log = slog.Default()
	}
}

// Server accepts WebSocket clients and forwards ACP frames.
type Server struct {
	cfg     Config
	factory HandlerFactory
	log     *slog.Logger
	up      websocket.Upgrader

	mu    sync.Mutex
	slots map[string]*Slot
}

// NewServer creates a server. It validates that a non-loopback bind has a
// token configured.
func NewServer(cfg Config, factory HandlerFactory) (*Server, error) {
	cfg.applyDefaults()

	host, _, err := net.SplitHostPort(cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("invalid addr %q: %w", cfg.Addr, err)
	}
	if cfg.Token == "" && !isLoopbackHost(host) {
		return nil, fmt.Errorf("refusing to bind %q without a token; set --token or bind to loopback", cfg.Addr)
	}

	return &Server{
		cfg:     cfg,
		factory: factory,
		log:     cfg.Log,
		slots:   make(map[string]*Slot),
		up: websocket.Upgrader{
			ReadBufferSize:  4096,
			WriteBufferSize: 4096,
			Subprotocols:    []string{"acp.v1"},
		},
	}, nil
}

// Run listens until ctx is cancelled.
func (s *Server) Run(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return err
	}
	httpSrv := &http.Server{Handler: s}
	errCh := make(chan error, 1)

	s.log.Info("claude-acp listening", "addr", s.cfg.Addr, "tls", s.cfg.TLSCert != "")
	go func() {
		if s.cfg.TLSCert != "" {
			errCh <- httpSrv.ServeTLS(ln, s.cfg.TLSCert, s.cfg.TLSKey)
			return
		}
		errCh <- httpSrv.Serve(ln)
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_ = httpSrv.Shutdown(shutdownCtx)
		cancel()
		// Wait for Serve to return, then terminate sessions synchronously so
		// CLI process groups are reaped before the process exits.
		<-errCh
		s.closeAllSlots()
		return nil
	case err := <-errCh:
		s.closeAllSlots()
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
		return
	}
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	if !s.authorized(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	conn, err := s.up.Upgrade(w, r, nil)
	if err != nil {
		s.log.Debug("websocket upgrade failed", "err", err)
		return
	}

	cid := r.Header.Get("X-Client-Id")
	slot, reconnect := s.slotFor(cid)
	p := slot.attach(conn, reconnect)
	if p == nil {
		return
	}
	s.log.Debug("client attached", "clientId", slot.id, "reconnect", reconnect, "remote", r.RemoteAddr)
	slot.readPump(p)
	s.log.Debug("client detached", "clientId", slot.id)
}

func (s *Server) authorized(r *http.Request) bool {
	if s.cfg.Token == "" {
		return true
	}
	got := r.Header.Get("Authorization")
	want := "Bearer " + s.cfg.Token
	return subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}

// slotFor returns an existing live slot for cid, or creates a new one.
func (s *Server) slotFor(cid string) (*Slot, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if cid != "" {
		if slot := s.slots[cid]; slot != nil && !slot.isClosed() {
			return slot, true
		}
	}
	id := cid
	if id == "" {
		id = newClientID()
	}
	slot := newSlot(id, s)
	s.slots[id] = slot
	return slot, false
}

func (s *Server) removeSlot(slot *Slot) {
	s.mu.Lock()
	if s.slots[slot.id] == slot {
		delete(s.slots, slot.id)
	}
	s.mu.Unlock()
}

// Shutdown terminates all logical clients and their sessions.
func (s *Server) Shutdown() {
	s.closeAllSlots()
}

func (s *Server) closeAllSlots() {
	s.mu.Lock()
	slots := make([]*Slot, 0, len(s.slots))
	for _, slot := range s.slots {
		slots = append(slots, slot)
	}
	s.mu.Unlock()

	for _, slot := range slots {
		slot.closeWithReason("server shutdown")
	}
}

func isLoopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func newClientID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("client-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}
