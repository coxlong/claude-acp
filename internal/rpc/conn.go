package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Handler processes an inbound request. A request with no ID is a
// notification; its result and error are ignored.
type Handler interface {
	Handle(ctx context.Context, req *Request) (any, *Error)
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc func(ctx context.Context, req *Request) (any, *Error)

// Handle implements Handler.
func (f HandlerFunc) Handle(ctx context.Context, req *Request) (any, *Error) {
	return f(ctx, req)
}

// Option customizes a Conn.
type Option func(*Conn)

// WithOnSend registers a hook invoked after every response is written to the
// peer. It is used by the WebSocket layer to cache responses for replay
// deduplication.
func WithOnSend(f func(*Response)) Option {
	return func(c *Conn) { c.onSend = f }
}

// WithLogger sets the diagnostic logger.
func WithLogger(l *slog.Logger) Option {
	return func(c *Conn) { c.log = l }
}

// Conn is a JSON-RPC 2.0 connection over an ObjectStream. It is symmetric:
// both peers may issue calls and handle incoming requests on the same Conn.
type Conn struct {
	stream  ObjectStream
	handler Handler
	log     *slog.Logger

	ctx    context.Context
	cancel context.CancelCauseFunc

	closed    chan struct{}
	closeOnce sync.Once

	sendMu sync.Mutex

	mu       sync.Mutex
	pending  map[string]chan *Response
	inflight map[string]context.CancelCauseFunc
	nextID   atomic.Uint64

	onSend func(*Response)
}

// NewConn starts a connection. Inbound requests are dispatched to handler.
// The context governs the lifetime of the connection.
func NewConn(ctx context.Context, stream ObjectStream, handler Handler, opts ...Option) *Conn {
	ctx, cancel := context.WithCancelCause(ctx)
	c := &Conn{
		stream:   stream,
		handler:  handler,
		log:      slog.Default(),
		ctx:      ctx,
		cancel:   cancel,
		closed:   make(chan struct{}),
		pending:  make(map[string]chan *Response),
		inflight: make(map[string]context.CancelCauseFunc),
	}
	for _, opt := range opts {
		opt(c)
	}
	go c.readLoop()
	return c
}

// Done is closed when the connection terminates.
func (c *Conn) Done() <-chan struct{} { return c.closed }

// Err reports why the connection closed.
func (c *Conn) Err() error { return context.Cause(c.ctx) }

// Call sends a request and waits for its response. If ctx is cancelled before
// the response arrives, a $/cancel_request notification is sent and ctx's
// error is returned.
func (c *Conn) Call(ctx context.Context, method string, params, result any) error {
	rawParams, err := marshalParams(params)
	if err != nil {
		return err
	}
	id := json.RawMessage(strconv.FormatUint(c.nextID.Add(1), 10))
	key := idKey(id)
	ch := make(chan *Response, 1)

	select {
	case <-c.closed:
		return ErrClosed
	default:
	}

	c.mu.Lock()
	c.pending[key] = ch
	c.mu.Unlock()

	req := &Request{JSONRPC: Version, ID: id, Method: method, Params: rawParams}
	if err := c.write(req); err != nil {
		c.removePending(key)
		return err
	}

	select {
	case resp := <-ch:
		if resp.Error != nil {
			return resp.Error
		}
		if result != nil && len(resp.Result) > 0 {
			if err := json.Unmarshal(resp.Result, result); err != nil {
				return ErrInternalMsg("invalid response result: " + err.Error())
			}
		}
		return nil
	case <-ctx.Done():
		c.removePending(key)
		// Best effort: ask the peer to stop working on the request.
		_ = c.Notify(context.Background(), "$/cancel_request", map[string]any{"requestId": id})
		return ctx.Err()
	case <-c.closed:
		return ErrClosed
	}
}

// Notify sends a notification. It returns once the message has been handed to
// the stream.
func (c *Conn) Notify(ctx context.Context, method string, params any) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.closed:
		return ErrClosed
	default:
	}
	rawParams, err := marshalParams(params)
	if err != nil {
		return err
	}
	return c.write(&Request{JSONRPC: Version, Method: method, Params: rawParams})
}

// FailPending fails every pending outbound call. It is used when a frame
// arriving from the peer cannot be parsed and may have been a response.
func (c *Conn) FailPending(err *Error) {
	c.mu.Lock()
	pending := c.pending
	c.pending = make(map[string]chan *Response)
	c.mu.Unlock()

	for _, ch := range pending {
		select {
		case ch <- &Response{JSONRPC: Version, Error: err}:
		default:
		}
	}
}

// Close terminates the connection.
func (c *Conn) Close() error {
	c.shutdown(ErrClosed)
	return nil
}

func (c *Conn) readLoop() {
	defer func() {
		c.shutdown(errors.New("read loop ended"))
	}()
	for {
		var raw json.RawMessage
		if err := c.stream.ReadObject(&raw); err != nil {
			if !errors.Is(err, io.EOF) {
				c.log.Debug("rpc: read failed", "err", err)
			}
			return
		}
		var probe struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
			Params json.RawMessage `json:"params"`
			Result json.RawMessage `json:"result"`
			Error  *Error          `json:"error"`
		}
		if err := json.Unmarshal(raw, &probe); err != nil {
			c.log.Warn("rpc: malformed message", "err", err)
			continue
		}
		switch {
		case probe.Method != "":
			c.handleIncoming(probe.ID, probe.Method, probe.Params)
		case len(probe.ID) > 0:
			key := idKey(probe.ID)
			c.mu.Lock()
			ch := c.pending[key]
			delete(c.pending, key)
			c.mu.Unlock()
			if ch == nil {
				c.log.Debug("rpc: response for unknown request", "id", string(probe.ID))
				continue
			}
			ch <- &Response{
				JSONRPC: Version,
				ID:      probe.ID,
				Result:  probe.Result,
				Error:   probe.Error,
			}
		default:
			c.log.Warn("rpc: message has neither method nor id")
		}
	}
}

func (c *Conn) handleIncoming(id json.RawMessage, method string, params json.RawMessage) {
	if method == "$/cancel_request" {
		var p struct {
			RequestID json.RawMessage `json:"requestId"`
		}
		if err := json.Unmarshal(params, &p); err != nil {
			c.log.Warn("rpc: malformed $/cancel_request", "err", err)
			return
		}
		c.cancelInflight(p.RequestID)
		return
	}

	req := &Request{JSONRPC: Version, ID: id, Method: method, Params: params}

	if req.IsNotification() {
		go func() {
			defer c.recoverPanic("notification " + method)
			_, _ = c.handler.Handle(c.ctx, req)
		}()
		return
	}

	key := idKey(id)
	ctx, cancel := context.WithCancelCause(c.ctx)
	c.mu.Lock()
	if _, exists := c.inflight[key]; exists {
		c.mu.Unlock()
		cancel(nil)
		// A concurrent duplicate id would silently overwrite the inflight
		// entry, leaving the first request uncancellable.
		c.writeResponse(&Response{JSONRPC: Version, ID: id, Error: &Error{
			Code:    -32600,
			Message: "request id already in flight",
		}})
		return
	}
	c.inflight[key] = cancel
	c.mu.Unlock()

	go func() {
		result, rerr := c.invoke(ctx, req)
		if ctx.Err() != nil {
			// A $/cancel_request (or connection shutdown) won the race.
			rerr = ErrCancelled
		}
		if rerr != nil {
			c.writeResponse(&Response{JSONRPC: Version, ID: id, Error: rerr})
		} else {
			raw, err := marshalResult(result)
			if err != nil {
				c.writeResponse(&Response{JSONRPC: Version, ID: id, Error: ErrInternalMsg(err.Error())})
			} else {
				c.writeResponse(&Response{JSONRPC: Version, ID: id, Result: raw})
			}
		}
		// Release the request's child context: without this, every completed
		// request leaks a live context for the connection's lifetime.
		c.mu.Lock()
		delete(c.inflight, key)
		c.mu.Unlock()
		cancel(nil)
	}()
}

// invoke runs the handler with panic recovery so a panicking handler still
// produces an error response instead of leaving the request unanswered
// forever.
func (c *Conn) invoke(ctx context.Context, req *Request) (result any, rerr *Error) {
	defer func() {
		if r := recover(); r != nil {
			c.log.Error("rpc: handler panic", "method", req.Method, "panic", r)
			result, rerr = nil, ErrInternalMsg("handler panic")
		}
	}()
	return c.handler.Handle(ctx, req)
}

func (c *Conn) cancelInflight(id json.RawMessage) {
	if len(id) == 0 {
		return
	}
	key := idKey(id)
	c.mu.Lock()
	cancel := c.inflight[key]
	c.mu.Unlock()
	if cancel != nil {
		cancel(ErrCancelled)
	}
}

func (c *Conn) removePending(key string) {
	c.mu.Lock()
	delete(c.pending, key)
	c.mu.Unlock()
}

func (c *Conn) write(v any) error {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	select {
	case <-c.closed:
		return ErrClosed
	default:
	}
	return c.stream.WriteObject(v)
}

func (c *Conn) writeResponse(resp *Response) {
	if err := c.write(resp); err != nil {
		c.log.Debug("rpc: response write failed", "err", err)
		return
	}
	if c.onSend != nil {
		c.onSend(resp)
	}
}

func (c *Conn) shutdown(cause error) {
	c.closeOnce.Do(func() {
		c.cancel(cause)
		close(c.closed)
		_ = c.stream.Close()

		c.mu.Lock()
		pending := c.pending
		c.pending = make(map[string]chan *Response)
		inflight := c.inflight
		c.inflight = make(map[string]context.CancelCauseFunc)
		c.mu.Unlock()

		for _, cancel := range inflight {
			cancel(cause)
		}
		for _, ch := range pending {
			select {
			case ch <- &Response{JSONRPC: Version, Error: ErrClosed}:
			default:
			}
		}
	})
}

func (c *Conn) recoverPanic(what string) {
	if r := recover(); r != nil {
		c.log.Error("rpc: handler panic", "what", what, "panic", r)
	}
}

func idKey(id json.RawMessage) string { return string(id) }

// WithTimeout is a convenience wrapper for calls that should not hang.
func WithTimeout(d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), d)
}
