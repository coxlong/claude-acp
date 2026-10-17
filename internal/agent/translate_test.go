package agent

import (
	"encoding/json"
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
		{Name: "fs", Command: "npx", Args: []string{"server"}, Env: map[string]string{"A": "B"}},
	})
	if !strings.Contains(got, `"mcpServers"`) || !strings.Contains(got, `"fs"`) {
		t.Fatalf("mcp config = %s", got)
	}
	if mcpConfigArg(nil) != "" {
		t.Fatal("empty server list should produce no flag")
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
