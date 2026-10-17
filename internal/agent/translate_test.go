package agent

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/coxlong/claude-acp/internal/acp"
	"github.com/coxlong/claude-acp/internal/claude"
)

func TestToolKind(t *testing.T) {
	cases := map[string]string{
		"Read":      acp.ToolKindRead,
		"Edit":      acp.ToolKindEdit,
		"Write":     acp.ToolKindEdit,
		"Bash":      acp.ToolKindExecute,
		"Grep":      acp.ToolKindSearch,
		"Glob":      acp.ToolKindSearch,
		"WebFetch":  acp.ToolKindFetch,
		"WebSearch": acp.ToolKindFetch,
		"Task":      acp.ToolKindOther,
		"mcp__x__y": acp.ToolKindOther,
	}
	for name, want := range cases {
		if got := toolKind(name); got != want {
			t.Fatalf("toolKind(%q) = %q, want %q", name, got, want)
		}
	}
}

func TestToolTitle(t *testing.T) {
	if got := toolTitle("Read", map[string]any{"file_path": "/tmp/a.go"}); got != "Read /tmp/a.go" {
		t.Fatalf("Read title = %q", got)
	}
	if got := toolTitle("Bash", map[string]any{"command": "ls -la"}); got != "Run: ls -la" {
		t.Fatalf("Bash title = %q", got)
	}
	if got := toolTitle("Unknown", nil); got != "Unknown" {
		t.Fatalf("fallback title = %q", got)
	}
}

func TestContentText(t *testing.T) {
	text, hasTool := contentText(json.RawMessage(`"hello"`))
	if text != "hello" || hasTool {
		t.Fatalf("string content = %q, %v", text, hasTool)
	}

	raw := json.RawMessage(`[{"type":"text","text":"a"},{"type":"text","text":"b"},{"type":"tool_result","content":"x"}]`)
	text, hasTool = contentText(raw)
	if text != "a\nb" || !hasTool {
		t.Fatalf("block content = %q, %v", text, hasTool)
	}
}

func TestPromptText(t *testing.T) {
	got := promptText([]acp.ContentBlock{
		{Type: "text", Text: "first"},
		{Type: "image"},
		{Type: "text", Text: "second"},
	})
	if got != "first\nsecond" {
		t.Fatalf("promptText = %q", got)
	}
}

func TestMCPConfigArg(t *testing.T) {
	got := mcpConfigArg([]acp.McpServer{
		{Name: "fs", Command: "npx", Args: []string{"server"}, Env: []acp.EnvVariable{{Name: "A", Value: "B"}}},
	})
	if !strings.Contains(got, `"mcpServers"`) || !strings.Contains(got, `"fs"`) {
		t.Fatalf("mcp config = %s", got)
	}
	if mcpConfigArg(nil) != "" {
		t.Fatal("empty server list should produce no flag")
	}
}

func TestMcpServerDecode(t *testing.T) {
	// Shapes taken from the official ACP schema: env and headers are arrays
	// of {name,value}, not objects.
	raw := `{"cwd":"/tmp","mcpServers":[
		{"name":"fs","command":"npx","args":[],"env":[{"name":"A","value":"B"}]},
		{"name":"web","type":"http","url":"https://x","headers":[{"name":"H","value":"V"}]}
	]}`
	var req acp.NewSessionRequest
	if err := json.Unmarshal([]byte(raw), &req); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(req.McpServers) != 2 || req.McpServers[0].Env[0].Value != "B" {
		t.Fatalf("servers = %#v", req.McpServers)
	}
	if req.McpServers[1].Headers[0].Name != "H" {
		t.Fatalf("headers = %#v", req.McpServers[1].Headers)
	}
}

func TestValidSessionID(t *testing.T) {
	valid := "d27cd207-8669-4f33-85a6-dda88fcd14a7"
	if !validSessionID(valid) {
		t.Fatal("valid uuid rejected")
	}
	for _, bad := range []string{"", "../../../tmp/secret", "d27cd20786694f3385a6dda88fcd14a7", "d27cd207-8669-4f33-85a6-dda88fcd14ag"} {
		if validSessionID(bad) {
			t.Fatalf("%q accepted", bad)
		}
	}
}

func TestValidateCwdRoot(t *testing.T) {
	root := t.TempDir()
	inside := filepath.Join(root, "project")
	if err := os.MkdirAll(inside, 0o755); err != nil {
		t.Fatal(err)
	}
	a := &Agent{cfg: Config{CwdRoot: root}}
	if err := a.validateCwd(inside); err != nil {
		t.Fatalf("inside root rejected: %v", err)
	}
	outside := t.TempDir()
	if err := a.validateCwd(outside); err == nil {
		t.Fatal("outside root accepted")
	}

	// The filesystem root is a valid (if unusual) allowlist root.
	rootAgent := &Agent{cfg: Config{CwdRoot: "/"}}
	if err := rootAgent.validateCwd("/tmp"); err != nil {
		t.Fatalf("cwd under / rejected: %v", err)
	}
}

func TestConcurrentPromptReturnsBusy(t *testing.T) {
	sess := &Session{id: "s1", mode: "default", toolCalls: map[string]bool{}}
	a := &Agent{sessions: map[string]*Session{"s1": sess}}
	sess.promptMu.Lock()
	defer sess.promptMu.Unlock()

	_, rerr := a.Prompt(context.Background(), acp.PromptRequest{
		SessionID: "s1",
		Prompt:    []acp.ContentBlock{{Type: "text", Text: "hello"}},
	})
	if rerr == nil || rerr.Code != -32602 {
		t.Fatalf("err = %v, want busy (-32602)", rerr)
	}
}

func TestResultEventMapping(t *testing.T) {
	a := &Agent{}
	sess := &Session{}

	done, stop, rerr := a.handleEvent(sess, &claude.ResultMessage{Subtype: "error_max_turns", IsError: true})
	if !done || stop != acp.StopReasonMaxTurnRequests || rerr != nil {
		t.Fatalf("max turns: done=%v stop=%q err=%v", done, stop, rerr)
	}

	done, stop, rerr = a.handleEvent(sess, &claude.ResultMessage{Subtype: "error_during_execution", IsError: true, Result: "boom"})
	if !done || stop != acp.StopReasonEndTurn || rerr == nil {
		t.Fatalf("execution error: done=%v stop=%q err=%v", done, stop, rerr)
	}
}

func TestResultStopReason(t *testing.T) {
	a := &Agent{}

	s := &Session{}
	if got := a.resultStopReason(s, &claude.ResultMessage{Subtype: "success"}); got != acp.StopReasonEndTurn {
		t.Fatalf("success = %q", got)
	}
	if got := a.resultStopReason(s, &claude.ResultMessage{Subtype: "error_max_turns"}); got != acp.StopReasonMaxTurnRequests {
		t.Fatalf("max turns = %q", got)
	}
	if got := a.resultStopReason(s, &claude.ResultMessage{StopReason: "max_tokens"}); got != acp.StopReasonMaxTokens {
		t.Fatalf("max tokens = %q", got)
	}
	if got := a.resultStopReason(s, &claude.ResultMessage{StopReason: "refusal"}); got != acp.StopReasonRefusal {
		t.Fatalf("refusal = %q", got)
	}
	s.markCancelled()
	if got := a.resultStopReason(s, &claude.ResultMessage{Subtype: "success"}); got != acp.StopReasonCancelled {
		t.Fatalf("cancelled = %q", got)
	}
}
