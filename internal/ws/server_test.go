package ws_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coxlong/claude-acp/internal/acp"
	"github.com/coxlong/claude-acp/internal/rpc"
	"github.com/coxlong/claude-acp/internal/ws"
	"github.com/gorilla/websocket"
)

type stubAgent struct {
	conn *rpc.Conn

	mu    sync.Mutex
	calls map[string]int
}

func newStub(conn *rpc.Conn) *stubAgent {
	return &stubAgent{conn: conn, calls: make(map[string]int)}
}

func (s *stubAgent) count(method string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls[method]++
	return s.calls[method]
}

func (s *stubAgent) callCount(method string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[method]
}

func (s *stubAgent) Initialize(context.Context, acp.InitializeRequest) (acp.InitializeResponse, *rpc.Error) {
	s.count("initialize")
	return acp.InitializeResponse{
		ProtocolVersion: acp.ProtocolVersion,
		AuthMethods:     []acp.AuthMethod{},
	}, nil
}

func (s *stubAgent) Authenticate(context.Context, acp.AuthenticateRequest) (acp.AuthenticateResponse, *rpc.Error) {
	return acp.AuthenticateResponse{}, nil
}

func (s *stubAgent) NewSession(context.Context, acp.NewSessionRequest) (acp.NewSessionResponse, *rpc.Error) {
	return acp.NewSessionResponse{SessionID: "s1"}, nil
}

func (s *stubAgent) LoadSession(context.Context, acp.LoadSessionRequest) (acp.LoadSessionResponse, *rpc.Error) {
	return acp.LoadSessionResponse{}, nil
}

func (s *stubAgent) ListSessions(context.Context, acp.ListSessionsRequest) (acp.ListSessionsResponse, *rpc.Error) {
	return acp.ListSessionsResponse{}, nil
}

func (s *stubAgent) CloseSession(context.Context, acp.CloseSessionRequest) (acp.CloseSessionResponse, *rpc.Error) {
	return acp.CloseSessionResponse{}, nil
}

func (s *stubAgent) Prompt(context.Context, acp.PromptRequest) (acp.PromptResponse, *rpc.Error) {
	return acp.PromptResponse{StopReason: acp.StopReasonEndTurn}, nil
}

func (s *stubAgent) Cancel(context.Context, acp.CancelNotification) *rpc.Error { return nil }

func (s *stubAgent) SetSessionMode(context.Context, acp.SetSessionModeRequest) (acp.SetSessionModeResponse, *rpc.Error) {
	return acp.SetSessionModeResponse{}, nil
}

func (s *stubAgent) SetSessionConfigOption(context.Context, acp.SetSessionConfigOptionRequest) (acp.SetSessionConfigOptionResponse, *rpc.Error) {
	return acp.SetSessionConfigOptionResponse{}, nil
}

func newTestServer(t *testing.T, token string, grace time.Duration) (*httptest.Server, <-chan *stubAgent) {
	t.Helper()
	agents := make(chan *stubAgent, 8)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	srv, err := ws.NewServer(ws.Config{
		Addr:         "127.0.0.1:0",
		Token:        token,
		SessionGrace: grace,
		Log:          logger,
	}, func(conn *rpc.Conn) rpc.Handler {
		a := newStub(conn)
		agents <- a
		return acp.NewServer(a)
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	ts := httptest.NewServer(srv)
	t.Cleanup(func() {
		ts.Close()
		srv.Shutdown()
	})
	return ts, agents
}

func dial(t *testing.T, ts *httptest.Server, clientID string, headers map[string]string) (*websocket.Conn, *http.Response) {
	t.Helper()
	url := "ws" + strings.TrimPrefix(ts.URL, "http")
	h := http.Header{}
	if clientID != "" {
		h.Set("X-Client-Id", clientID)
	}
	for k, v := range headers {
		h.Set(k, v)
	}
	conn, resp, err := websocket.DefaultDialer.Dial(url, h)
	if err != nil {
		return nil, resp
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn, resp
}

func readJSON(t *testing.T, conn *websocket.Conn) map[string]any {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, data, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("decode %s: %v", data, err)
	}
	return m
}

func writeJSON(t *testing.T, conn *websocket.Conn, v any) {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if err := conn.WriteMessage(websocket.TextMessage, b); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func TestInitialize(t *testing.T) {
	ts, agents := newTestServer(t, "", 10*time.Second)
	conn, _ := dial(t, ts, "", nil)

	preamble := readJSON(t, conn)
	if preamble["type"] != "connected" || preamble["clientId"] == "" {
		t.Fatalf("unexpected preamble: %#v", preamble)
	}

	writeJSON(t, conn, map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "initialize",
		"params":  map[string]any{"protocolVersion": 1},
	})
	resp := readJSON(t, conn)
	result, _ := resp["result"].(map[string]any)
	if result == nil || result["protocolVersion"] != float64(1) {
		t.Fatalf("unexpected response: %#v", resp)
	}
	a := <-agents
	if got := a.callCount("initialize"); got != 1 {
		t.Fatalf("initialize calls = %d, want 1", got)
	}
}

func TestReconnectReplaysOutbox(t *testing.T) {
	ts, agents := newTestServer(t, "", 10*time.Second)

	conn, _ := dial(t, ts, "client-a", nil)
	preamble := readJSON(t, conn)
	if preamble["type"] != "connected" {
		t.Fatalf("preamble = %#v", preamble)
	}
	a := <-agents

	_ = conn.Close()
	time.Sleep(100 * time.Millisecond)

	// Produce output while no physical connection is attached.
	err := a.conn.Notify(context.Background(), acp.ClientSessionUpdate, acp.SessionNotification{
		SessionID: "s1",
		Update: acp.SessionUpdate{
			SessionUpdate: acp.UpdateAgentMessageChunk,
			Content:       acp.TextContent("buffered"),
		},
	})
	if err != nil {
		t.Fatalf("notify: %v", err)
	}

	conn2, _ := dial(t, ts, "client-a", nil)
	preamble = readJSON(t, conn2)
	if preamble["type"] != "reconnect" {
		t.Fatalf("preamble = %#v, want reconnect", preamble)
	}
	replayed := readJSON(t, conn2)
	if replayed["method"] != acp.ClientSessionUpdate {
		t.Fatalf("replayed = %#v", replayed)
	}
	params, _ := replayed["params"].(map[string]any)
	update, _ := params["update"].(map[string]any)
	if update == nil || update["sessionUpdate"] != acp.UpdateAgentMessageChunk {
		t.Fatalf("update = %#v", update)
	}
}

func TestDuplicateRequestIsReplayedNotReexecuted(t *testing.T) {
	ts, agents := newTestServer(t, "", 10*time.Second)
	conn, _ := dial(t, ts, "dedupe", nil)
	_ = readJSON(t, conn)
	a := <-agents

	req := map[string]any{
		"jsonrpc": "2.0",
		"id":      7,
		"method":  "initialize",
		"params":  map[string]any{"protocolVersion": 1},
	}
	writeJSON(t, conn, req)
	first := readJSON(t, conn)
	if first["result"] == nil {
		t.Fatalf("first response = %#v", first)
	}

	writeJSON(t, conn, req)
	second := readJSON(t, conn)
	if second["result"] == nil {
		t.Fatalf("second response = %#v", second)
	}

	time.Sleep(100 * time.Millisecond)
	if got := a.callCount("initialize"); got != 1 {
		t.Fatalf("initialize calls = %d, want 1 (duplicate must not re-execute)", got)
	}
}

func TestMalformedFrameDoesNotKillClient(t *testing.T) {
	ts, _ := newTestServer(t, "", 10*time.Second)
	conn, _ := dial(t, ts, "", nil)
	_ = readJSON(t, conn)

	if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"jsonrpc":`)); err != nil {
		t.Fatalf("write malformed frame: %v", err)
	}

	writeJSON(t, conn, map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "initialize",
		"params":  map[string]any{"protocolVersion": 1},
	})
	resp := readJSON(t, conn)
	if resp["result"] == nil {
		t.Fatalf("client did not recover from malformed frame: %#v", resp)
	}
}

func TestAuth(t *testing.T) {
	ts, _ := newTestServer(t, "secret", 10*time.Second)

	if _, resp := dial(t, ts, "", nil); resp == nil || resp.StatusCode != http.StatusUnauthorized {
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		t.Fatalf("unauthenticated status = %d, want 401", status)
	}

	conn, resp := dial(t, ts, "", map[string]string{"Authorization": "Bearer secret"})
	if conn == nil {
		t.Fatalf("authenticated dial failed: %v", resp)
	}
	if p := readJSON(t, conn); p["type"] != "connected" {
		t.Fatalf("preamble = %#v", p)
	}
}

func TestNonLoopbackRequiresToken(t *testing.T) {
	factory := func(*rpc.Conn) rpc.Handler { return acp.NewServer(newStub(nil)) }
	if _, err := ws.NewServer(ws.Config{Addr: "0.0.0.0:3000"}, factory); err == nil {
		t.Fatal("expected error for non-loopback bind without token")
	}
	if _, err := ws.NewServer(ws.Config{Addr: "127.0.0.1:3000"}, factory); err != nil {
		t.Fatalf("loopback bind should be allowed: %v", err)
	}
}
