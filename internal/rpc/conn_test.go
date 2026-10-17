package rpc_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
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
