package ws

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"regexp"
	"sync"
	"time"

	"github.com/coxlong/claude-acp/internal/acp"
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

	// seenIDs tracks every request id observed on this logical client, across
	// reconnects. A replayed id is answered from the dedupe cache; an id that
	// was never seen belongs to a restarted client process whose counter
	// reset — its stale same-numbered entry must be ignored.
	seenIDs   map[string]bool
	seenOrder []string

	conn    *rpc.Conn
	handler rpc.Handler
	ready   chan struct{}
	readyAt sync.Once

	done      chan struct{}
	closeOnce sync.Once

	// connMu serializes physical connection handoff (attach/detach/close) so
	// an old writer cannot requeue frames after a new writer started.
	connMu sync.Mutex

	mu     sync.Mutex
	phys   *physical
	gen    uint64
	grace  *time.Timer
	closed bool

	// secret is returned in the preamble and (with --strict-client-id)
	// required on reconnect, so a guessed client id cannot hijack a live
	// client's slot.
	secret string
}

func newSlot(id string, srv *Server) *Slot {
	s := &Slot{
		id:      id,
		srv:     srv,
		log:     srv.log.With("clientId", id),
		out:     newOutbox(srv.cfg.OutboxMax),
		inbound: make(chan []byte, 1024),
		dedupe:  newDedupe(256),
		seenIDs: make(map[string]bool),
		ready:   make(chan struct{}),
		done:    make(chan struct{}),
		secret:  newClientID(),
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
				s.handleMalformedFrame(frame, err)
				continue
			}
			return nil
		}
	}
}

// handleMalformedFrame deals with a frame that is not valid JSON. A request
// whose id can be recovered gets a -32700 parse error so the caller does not
// hang; likely responses fail pending agent calls; everything else is logged.
func (s *Slot) handleMalformedFrame(frame []byte, err error) {
	// The frame failed strict decoding (it may be truncated), so recover the
	// id/method leniently before deciding what to answer.
	id := lenientScan(frame, `"id"\s*:\s*(-?\d+|"(?:[^"\\]|\\.)*")`)
	method := lenientScan(frame, `"method"\s*:\s*"((?:[^"\\]|\\.)*)"`)

	if id != "" && method != "" {
		s.log.Warn("malformed request frame; answering -32700", "err", err)
		resp := rpc.Response{JSONRPC: rpc.Version, ID: json.RawMessage(id), Error: rpc.ErrParse}
		if b, mErr := json.Marshal(resp); mErr == nil {
			_ = s.enqueue(b)
		}
		return
	}
	if id != "" {
		// A corrupted response to an agent -> client call: fail pending
		// calls so the CLI is not left waiting for an answer that will
		// never arrive.
		s.log.Warn("malformed response frame; failing pending calls", "err", err)
		s.conn.FailPending(rpc.ErrInvalidRequest)
		return
	}
	if method != "" || bytes.Contains(frame, []byte(`"method"`)) {
		s.log.Warn("dropping malformed request frame (no recoverable id)", "err", err)
		return
	}
	s.log.Warn("dropping malformed frame; failing pending calls", "err", err)
	s.conn.FailPending(rpc.ErrInvalidRequest)
}

// lenientScan extracts the first capture of pattern from a possibly
// malformed JSON frame. An empty result means "not found".
func lenientScan(frame []byte, pattern string) string {
	re := lazyPattern(pattern)
	m := re.FindSubmatch(frame)
	if m == nil {
		return ""
	}
	return string(m[1])
}

var patternCache sync.Map // string -> *regexp.Regexp

func lazyPattern(pattern string) *regexp.Regexp {
	if v, ok := patternCache.Load(pattern); ok {
		return v.(*regexp.Regexp)
	}
	re := regexp.MustCompile(pattern)
	patternCache.Store(pattern, re)
	return re
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

// enqueue delivers a frame to the outbound buffer. On overflow the design's
// policy applies: clear the buffer and terminate the sessions, but keep the
// logical client alive — destroying the slot would defeat the whole
// grace/replay mechanism it exists to provide.
func (s *Slot) enqueue(frame []byte) error {
	if !s.out.push(frame) {
		s.handleOverflow("outbox overflow while enqueuing")
	}
	return nil
}

// handleOverflow implements DESIGN.md 6.6: on unbounded output, cancel the
// active turn (by terminating its sessions), clear the outbox, and continue
// serving the client.
func (s *Slot) handleOverflow(reason string) {
	s.log.Error("outbox overflow; clearing buffer and terminating sessions", "reason", reason)
	s.out.clear()
	if t, ok := s.handler.(acp.SessionTerminator); ok {
		t.TerminateSessions()
	}
}

func (s *Slot) closeWithReason(reason string) {
	s.closeOnce.Do(func() {
		s.log.Info("slot closing", "reason", reason)

		s.connMu.Lock()
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
			close(p.done)
			_ = p.conn.Close()
			select {
			case <-p.writerDone:
			case <-time.After(2 * time.Second):
			}
		}
		s.connMu.Unlock()

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
	s.connMu.Lock()
	defer s.connMu.Unlock()

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
	old = s.phys
	if old != nil {
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
	preamble, _ := json.Marshal(map[string]string{"type": kind, "clientId": s.id, "secret": s.secret})
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
				if !s.out.requeue(frames[i:]) {
					s.handleOverflow("outbox overflow after write interrupted")
				}
				return
			default:
			}
			_ = p.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			if err := p.conn.WriteMessage(websocket.TextMessage, frame); err != nil {
				// Requeue undelivered frames for the next connection.
				if !s.out.requeue(frames[i:]) {
					s.handleOverflow("outbox overflow after write failure")
				}
				_ = p.conn.Close()
				return
			}
		}
	}
}

func (s *Slot) detach(p *physical) {
	s.connMu.Lock()

	s.mu.Lock()
	if s.phys != p {
		s.mu.Unlock()
		s.connMu.Unlock()
		return
	}
	s.phys = nil
	close(p.done)
	closed := s.closed
	s.mu.Unlock()

	// Close the socket and wait for the writer, so a reconnect cannot start
	// a new pump while this one is still able to requeue old frames.
	_ = p.conn.Close()
	select {
	case <-p.writerDone:
	case <-time.After(2 * time.Second):
	}

	grace := s.srv.cfg.SessionGrace
	if !closed {
		s.mu.Lock()
		if !s.closed && s.phys == nil && grace > 0 {
			s.grace = time.AfterFunc(grace, func() {
				s.closeWithReason("session grace expired")
			})
		}
		s.mu.Unlock()
	}
	s.connMu.Unlock()

	if !closed && grace <= 0 {
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
			id := string(probe.ID)
			known := s.markSeen(id)
			cached, duplicate := s.dedupe.begin(id, known)
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

// markSeen records a request id and reports whether it was already seen on
// this logical client. Ids seen before belong to the current client
// lifetime; an unseen id that collides with a stale entry belongs to a
// restarted client process and must be executed fresh.
func (s *Slot) markSeen(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	known := s.seenIDs[id]
	if !known {
		s.seenIDs[id] = true
		s.seenOrder = append(s.seenOrder, id)
		for len(s.seenOrder) > 1024 {
			oldest := s.seenOrder[0]
			s.seenOrder = s.seenOrder[1:]
			delete(s.seenIDs, oldest)
		}
	}
	return known
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
