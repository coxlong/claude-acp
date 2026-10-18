package rpc_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coxlong/claude-acp/internal/rpc"
)

// pipe is one direction of an in-memory ObjectStream.
type pipe struct {
	in     <-chan []byte
	out    chan<- []byte
	closed chan struct{}
}

func (p *pipe) ReadObject(v any) error {
	select {
	case b, ok := <-p.in:
		if !ok {
			return io.EOF
		}
		return json.Unmarshal(b, v)
	case <-p.closed:
		return io.EOF
	}
}

func (p *pipe) WriteObject(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	select {
	case p.out <- b:
		return nil
	case <-p.closed:
		return io.EOF
	}
}

// Close is a no-op: the test owns the shared closed channel.
func (p *pipe) Close() error { return nil }

func newPair(t *testing.T) (*rpc.Conn, *rpc.Conn, func()) {
	t.Helper()
	aToB := make(chan []byte, 64)
	bToA := make(chan []byte, 64)
	closed := make(chan struct{})
	var once sync.Once
	closeAll := func() { once.Do(func() { close(closed) }) }

	// Conn A reads from bToA, writes to aToB.
	streamA := &pipe{in: bToA, out: aToB, closed: closed}
	// Conn B reads from aToB, writes to bToA.
	streamB := &pipe{in: aToB, out: bToA, closed: closed}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	a := rpc.NewConn(ctx, streamA, rpc.HandlerFunc(func(ctx context.Context, req *rpc.Request) (any, *rpc.Error) {
		var p map[string]any
		if err := req.DecodeParams(&p); err != nil {
			return nil, rpc.ErrInvalidParams(err.Error())
		}
		return p, nil
	}))
	b := rpc.NewConn(ctx, streamB, rpc.HandlerFunc(func(ctx context.Context, req *rpc.Request) (any, *rpc.Error) {
		var p map[string]any
		if err := req.DecodeParams(&p); err != nil {
			return nil, rpc.ErrInvalidParams(err.Error())
		}
		return p, nil
	}))
	return a, b, closeAll
}

func TestCallRoundTrip(t *testing.T) {
	a, _, closeAll := newPair(t)
	defer closeAll()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var got map[string]any
	if err := a.Call(ctx, "echo", map[string]any{"x": float64(1)}, &got); err != nil {
		t.Fatalf("call: %v", err)
	}
	if got["x"] != float64(1) {
		t.Fatalf("unexpected result: %#v", got)
	}
}

func TestNotification(t *testing.T) {
	aToB := make(chan []byte, 8)
	bToA := make(chan []byte, 8)
	closed := make(chan struct{})
	defer close(closed)

	got := make(chan string, 1)
	a := rpc.NewConn(context.Background(), &pipe{in: bToA, out: aToB, closed: closed}, rpc.HandlerFunc(
		func(ctx context.Context, req *rpc.Request) (any, *rpc.Error) { return nil, nil },
	))
	b := rpc.NewConn(context.Background(), &pipe{in: aToB, out: bToA, closed: closed}, rpc.HandlerFunc(
		func(ctx context.Context, req *rpc.Request) (any, *rpc.Error) {
			got <- req.Method
			return nil, nil
		},
	))

	if err := a.Notify(context.Background(), "session/update", map[string]any{"a": 1}); err != nil {
		t.Fatalf("notify: %v", err)
	}
	select {
	case m := <-got:
		if m != "session/update" {
			t.Fatalf("method = %q", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("notification not delivered")
	}
	_ = b
}

func TestCallCancellation(t *testing.T) {
	aToB := make(chan []byte, 8)
	bToA := make(chan []byte, 8)
	closed := make(chan struct{})
	defer close(closed)

	started := make(chan struct{})
	cancelled := make(chan struct{})
	var once sync.Once

	a := rpc.NewConn(context.Background(), &pipe{in: bToA, out: aToB, closed: closed}, rpc.HandlerFunc(
		func(ctx context.Context, req *rpc.Request) (any, *rpc.Error) { return nil, nil },
	))
	_ = rpc.NewConn(context.Background(), &pipe{in: aToB, out: bToA, closed: closed}, rpc.HandlerFunc(
		func(ctx context.Context, req *rpc.Request) (any, *rpc.Error) {
			once.Do(func() { close(started) })
			<-ctx.Done()
			close(cancelled)
			return nil, rpc.ErrCancelled
		},
	))

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := a.Call(ctx, "slow", nil, nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want deadline exceeded", err)
	}
	select {
	case <-started:
	default:
		t.Fatal("handler did not start")
	}
	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("handler context was not cancelled by $/cancel_request")
	}
}

func TestMethodNotFound(t *testing.T) {
	aToB := make(chan []byte, 8)
	bToA := make(chan []byte, 8)
	closed := make(chan struct{})
	defer close(closed)

	a := rpc.NewConn(context.Background(), &pipe{in: bToA, out: aToB, closed: closed}, rpc.HandlerFunc(
		func(ctx context.Context, req *rpc.Request) (any, *rpc.Error) { return nil, nil },
	))
	b := rpc.NewConn(context.Background(), &pipe{in: aToB, out: bToA, closed: closed}, rpc.HandlerFunc(
		func(ctx context.Context, req *rpc.Request) (any, *rpc.Error) {
			if req.Method != "known" {
				return nil, rpc.ErrMethodNotFound(req.Method)
			}
			return struct{}{}, nil
		},
	))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := a.Call(ctx, "unknown", nil, nil)
	var rerr *rpc.Error
	if !errors.As(err, &rerr) || rerr.Code != -32601 {
		t.Fatalf("err = %v, want -32601", err)
	}
	_ = b
}

func TestDuplicateInFlightIDRejected(t *testing.T) {
	aToB := make(chan []byte, 8)
	bToA := make(chan []byte, 8)
	closed := make(chan struct{})
	defer close(closed)

	release := make(chan struct{})
	streamA := &pipe{in: bToA, out: aToB, closed: closed}
	_ = streamA
	// Only conn B exists: its readLoop consumes aToB and its responses land
	// in bToA, which this test reads directly (no competing readLoop).
	_ = rpc.NewConn(context.Background(), &pipe{in: aToB, out: bToA, closed: closed}, rpc.HandlerFunc(
		func(ctx context.Context, req *rpc.Request) (any, *rpc.Error) {
			if req.Method == "slow" {
				<-release
			}
			return map[string]any{"method": req.Method}, nil
		},
	))

	sameID := func(method string) map[string]any {
		return map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": nil}
	}
	b, _ := json.Marshal(sameID("slow"))
	if err := (&pipe{in: bToA, out: aToB, closed: closed}).writeRaw(b); err != nil {
		t.Fatalf("write: %v", err)
	}
	b2, _ := json.Marshal(sameID("echo"))
	if err := (&pipe{in: bToA, out: aToB, closed: closed}).writeRaw(b2); err != nil {
		t.Fatalf("write dup: %v", err)
	}

	// The duplicate must get -32600 without waiting for the slow handler.
	_ = readRaw(bToA, 2*time.Second, t, `"request id already in flight"`)
	close(release)
}

func TestHandlerPanicReturnsInternalError(t *testing.T) {
	aToB := make(chan []byte, 8)
	bToA := make(chan []byte, 8)
	closed := make(chan struct{})
	defer close(closed)

	_ = rpc.NewConn(context.Background(), &pipe{in: aToB, out: bToA, closed: closed}, rpc.HandlerFunc(
		func(ctx context.Context, req *rpc.Request) (any, *rpc.Error) {
			panic("boom")
		},
	))

	b, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 5, "method": "explode", "params": nil})
	if err := (&pipe{in: bToA, out: aToB, closed: closed}).writeRaw(b); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = readRaw(bToA, 2*time.Second, t, `handler panic`)
}

// writeRaw sends a pre-marshaled frame on the test pipe.
func (p *pipe) writeRaw(b []byte) error {
	out := p.out
	select {
	case out <- b:
		return nil
	case <-p.closed:
		return io.EOF
	}
}

func readRaw(in <-chan []byte, timeout time.Duration, t *testing.T, want string) map[string]any {
	t.Helper()
	select {
	case b := <-in:
		var m map[string]any
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatalf("decode %s: %v", b, err)
		}
		if !strings.Contains(string(b), want) {
			t.Fatalf("frame %s does not contain %q", b, want)
		}
		return m
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for frame containing %q", want)
		return nil
	}
}
