package claude

import (
	"encoding/json"
	"slices"
	"testing"
)

func TestBuildArgs(t *testing.T) {
	args := BuildArgs(Options{SessionID: "abc", Model: "m1", PermissionMode: "plan", Effort: "high"})
	for _, want := range []string{
		"--output-format", "stream-json",
		"--input-format", "stream-json",
		"--include-partial-messages",
		"--permission-prompt-tool", "stdio",
		"--session-id=abc",
		"--model", "m1",
		"--permission-mode", "plan",
		"--effort", "high",
	} {
		if !slices.Contains(args, want) {
			t.Fatalf("args missing %q: %v", want, args)
		}
	}
}

func TestBuildArgsAllowBypass(t *testing.T) {
	args := BuildArgs(Options{PermissionMode: "manual", AllowBypass: true})
	if !slices.Contains(args, "--allow-dangerously-skip-permissions") {
		t.Fatalf("missing bypass enabler: %v", args)
	}
	if slices.Contains(args, "--dangerously-skip-permissions") {
		t.Fatalf("bypass enabler must not force the mode: %v", args)
	}
}

func TestBuildArgsResume(t *testing.T) {
	args := BuildArgs(Options{Resume: "old"})
	if !slices.Contains(args, "--resume=old") {
		t.Fatalf("missing resume: %v", args)
	}
	if slices.Contains(args, "--session-id=old") {
		t.Fatalf("resume must not also pass --session-id: %v", args)
	}
}

func TestResultMessageJSONTags(t *testing.T) {
	raw := `{"type":"result","subtype":"error_during_execution","is_error":true,` +
		`"result":"boom","stop_reason":"max_tokens","terminal_reason":"max_turns",` +
		`"session_id":"s1","num_turns":3}`
	var msg ResultMessage
	if err := json.Unmarshal([]byte(raw), &msg); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !msg.IsError || msg.StopReason != "max_tokens" || msg.TerminalReason != "max_turns" ||
		msg.SessionID != "s1" || msg.NumTurns != 3 || msg.Result != "boom" {
		t.Fatalf("decoded = %#v", msg)
	}
}

func TestOptionsValidate(t *testing.T) {
	if err := (Options{SessionID: "a", Resume: "b"}).Validate(); err == nil {
		t.Fatal("expected conflict error")
	}
	if err := (Options{PermissionMode: "nope"}).Validate(); err == nil {
		t.Fatal("expected invalid mode error")
	}
	if err := (Options{SessionID: "a", PermissionMode: "plan"}).Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, mode := range []string{"auto", "dontAsk", "manual"} {
		if err := (Options{PermissionMode: mode}).Validate(); err != nil {
			t.Fatalf("mode %q rejected: %v", mode, err)
		}
	}
	if err := (Options{PermissionMode: "default"}).Validate(); err == nil {
		t.Fatal(`"default" is no longer the wire id; "manual" is`)
	}
}
