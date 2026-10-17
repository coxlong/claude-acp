package claude

import (
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

func TestBuildArgsResume(t *testing.T) {
	args := BuildArgs(Options{Resume: "old"})
	if !slices.Contains(args, "--resume=old") {
		t.Fatalf("missing resume: %v", args)
	}
	if slices.Contains(args, "--session-id=old") {
		t.Fatalf("resume must not also pass --session-id: %v", args)
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
}
