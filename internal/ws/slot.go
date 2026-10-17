package ws

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/coxlong/claude-acp/internal/rpc"
	"github.com/gorilla/websocket"
)

const (
	writeTimeout = 30 * time.Second
	pongWait     = 90 * time.Second
	pingPeriod   = 30 * time.Second
)

type physical struct {
	id         uint64
	conn       *websocket.Conn
	done       chan struct{}
	writerDone chan struct{}
}

// Slot is one logical ACP client. It owns the JSON-RPC connection, the
// outbound buffer, and the sessions of that client. It survives physical
// WebSocket reconnects.
type Slot struct {
	id  string
	srv *Server
	log *slog.Logger

	out     *outbox
	inbound chan []byte
	dedupe  *dedupe

	conn    *rpc.Conn
	handler rpc.Handler
	ready   chan struct{}
	readyAt sync.Once

	done      chan struct{}
	closeOnce sync.Once

	mu     sync.Mutex
	phys   *physical
	gen    uint64
	grace  *time.Timer
	closed bool
}

func newSlot(id string, srv *Server) *Slot {
	s := &Slot{
		id:      id,
		srv:     srv,
		log:     srv.log.With("clientId", id),
		out:     newOutbox(srv.cfg.OutboxMax),
		inbound: make(chan []byte, 1024),
		dedupe:  newDedupe(256),
		ready:   make(chan struct{}),
		done:    make(chan struct{}),
	}
	s.conn = rpc.NewConn(
		context.Background(),
		s,
		s,
		rpc.WithOnSend(s.dedupe.recordResponse),
		rpc.WithLogger(s.log),
	)
	s.handler = srv.factory(s.conn)
	s.readyAt.Do(func() { close(s.ready) })
	return s
}

// Handle implements rpc.Handler. It waits until the agent handler has been
// constructed (which requires the connection itself).
func (s *Slot) Handle(ctx context.Context, req *rpc.Request) (any, *rpc.Error) {
	select {
	case <-s.ready:
	case <-s.done:
		return nil, rpc.ErrClosed
	}
	return s.handler.Handle(ctx, req)
}

// ReadObject implements rpc.ObjectStream. Malformed frames are dropped: a
// single bad frame must not tear down the logical client and its sessions.
func (s *Slot) ReadObject(v any) error {
	for {
		select {
		case <-s.done:
			return io.EOF
		default:
		}
		select {
		case <-s.done:
			return io.EOF
		case frame := <-s.inbound:
			if err := json.Unmarshal(frame, v); err != nil {
				s.log.Warn("dropping malformed frame", "err", err)
				continue
			}
			return nil
		}
	}
}

// WriteObject implements rpc.ObjectStream.
func (s *Slot) WriteObject(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return s.enqueue(b)
}

// Close implements rpc.ObjectStream and terminates the slot.
func (s *Slot) Close() error {
	s.closeWithReason("connection closed")
	return nil
}

func (s *Slot) enqueue(frame []byte) error {
	if !s.out.push(frame) {
		go s.closeWithReason("outbox overflow")
		return rpc.ErrClosed
	}
	return nil
}

func (s *Slot) closeWithReason(reason string) {
	s.closeOnce.Do(func() {
		s.log.Info("slot closing", "reason", reason)
		s.mu.Lock()
		s.closed = true
		if s.grace != nil {
			s.grace.Stop()
			s.grace = nil
		}
		p := s.phys
		s.phys = nil
		s.mu.Unlock()

		if p != nil {
			_ = p.conn.Close()
			close(p.done)
		}
		close(s.done)
		s.out.close()

		if closer, ok := s.handler.(io.Closer); ok {
			_ = closer.Close()
		}
		s.srv.removeSlot(s)
	})
}

func (s *Slot) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// attach binds a physical connection and starts its writer pump.
func (s *Slot) attach(conn *websocket.Conn, reconnect bool) *physical {
	var old *physical

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		_ = conn.Close()
		return nil
	}
	if s.grace != nil {
		s.grace.Stop()
		s.grace = nil
	}
	if s.phys != nil {
		old = s.phys
		close(old.done)
	}
	s.gen++
	p := &physical{id: s.gen, conn: conn, done: make(chan struct{}), writerDone: make(chan struct{})}
	s.phys = p
	s.mu.Unlock()

	// Let the previous writer finish before draining the shared outbox, so
	// two pumps cannot race for the same frames.
	if old != nil {
		_ = old.conn.Close()
		select {
		case <-old.writerDone:
		case <-time.After(2 * time.Second):
		}
	}

	kind := "connected"
	if reconnect {
		kind = "reconnect"
	}
	preamble, _ := json.Marshal(map[string]string{"type": kind, "clientId": s.id})
	// Write the preamble directly: it must precede any buffered replay.
	_ = p.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	if err := p.conn.WriteMessage(websocket.TextMessage, preamble); err != nil {
		_ = p.conn.Close()
		close(p.writerDone)
		return p
	}

	go s.writePump(p)
	return p
}

// readPump reads frames until the physical connection dies, then detaches.
func (s *Slot) readPump(p *physical) {
	defer s.detach(p)

	p.conn.SetReadLimit(s.srv.cfg.MaxFrameBytes)
	_ = p.conn.SetReadDeadline(time.Now().Add(pongWait))
	p.conn.SetPongHandler(func(string) error {
		return p.conn.SetReadDeadline(time.Now().Add(pongWait))
	})
	for {
		_, data, err := p.conn.ReadMessage()
		if err != nil {
			return
		}
		// Any inbound traffic (including the client's $/ping notifications)
		// proves the peer is alive.
		_ = p.conn.SetReadDeadline(time.Now().Add(pongWait))
		for _, line := range splitLines(data) {
			s.handleInboundLine(line)
		}
	}
}

func (s *Slot) writePump(p *physical) {
	defer close(p.writerDone)
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-p.done:
			return
		case <-s.done:
			return
		case <-ticker.C:
			_ = p.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			if err := p.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				_ = p.conn.Close()
				return
			}
		case <-s.out.signal():
		}

		frames := s.out.drain()
		for i, frame := range frames {
			select {
			case <-p.done:
				s.out.requeue(frames[i:])
				return
			default:
			}
			_ = p.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			if err := p.conn.WriteMessage(websocket.TextMessage, frame); err != nil {
				// Requeue undelivered frames for the next connection.
				s.out.requeue(frames[i:])
				_ = p.conn.Close()
				return
			}
		}
	}
}

func (s *Slot) detach(p *physical) {
	s.mu.Lock()
	if s.phys != p {
		s.mu.Unlock()
		return
	}
	s.phys = nil
	close(p.done)
	if s.closed {
		s.mu.Unlock()
		return
	}
	grace := s.srv.cfg.SessionGrace
	if grace > 0 {
		s.grace = time.AfterFunc(grace, func() {
			s.closeWithReason("session grace expired")
		})
	}
	s.mu.Unlock()

	if grace <= 0 {
		s.closeWithReason("disconnected")
	}
}

// handleInboundLine filters keep-alives, suppresses duplicate requests, and
// forwards everything else to the JSON-RPC layer.
func (s *Slot) handleInboundLine(line []byte) {
	select {
	case <-s.done:
		return
	default:
	}

	var probe struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
	}
	if err := json.Unmarshal(line, &probe); err == nil {
		if probe.Method == "$/ping" {
			return
		}
		if probe.Method != "" && len(probe.ID) > 0 {
			cached, duplicate := s.dedupe.begin(string(probe.ID))
			if cached != nil {
				_ = s.enqueue(cached)
				return
			}
			if duplicate {
				return
			}
		}
	}

	select {
	case s.inbound <- line:
	case <-s.done:
	}
}

func splitLines(data []byte) [][]byte {
	var out [][]byte
	for _, line := range bytes.Split(data, []byte{'\n'}) {
		line = bytes.TrimSpace(line)
		if len(line) > 0 {
			out = append(out, line)
		}
	}
	return out
}
