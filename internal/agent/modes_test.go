package agent

import (
	"context"
	"testing"

	"github.com/coxlong/claude-acp/internal/acp"
	"github.com/coxlong/claude-acp/internal/claude"
)

func modeIDs(modes []acp.SessionMode) []string {
	out := make([]string, 0, len(modes))
	for _, m := range modes {
		out = append(out, m.ID)
	}
	return out
}

func TestAvailableModes(t *testing.T) {
	s := &Session{
		model:  "auto-model",
		models: []claude.ModelInfo{{Value: "auto-model", SupportsAutoMode: true}},
	}
	modes := s.availableModesLocked()

	want := []string{"auto", "manual", "acceptEdits", "plan", "dontAsk"}
	if allowBypassPermissions() {
		want = append(want, "bypassPermissions")
	}
	got := modeIDs(modes)
	if len(got) != len(want) {
		t.Fatalf("modes = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("modes = %v, want %v", got, want)
		}
	}

	if modes[1].Name != "Manual" {
		t.Fatalf(`default mode name = %q, want "Manual"`, modes[1].Name)
	}
}

func TestAutoGatedByModel(t *testing.T) {
	s := &Session{
		model:  "plain-model",
		models: []claude.ModelInfo{{Value: "plain-model", SupportsAutoMode: false}},
	}
	if s.hasMode("auto") {
		t.Fatal("auto advertised for a model without auto support")
	}
	if !s.hasMode("dontAsk") || !s.hasMode("plan") || !s.hasMode("manual") {
		t.Fatal("unconditional modes must always be available")
	}
	if s.hasMode("default") {
		t.Fatal(`"default" must not be advertised; "manual" is the wire id`)
	}
}

func TestSetModelClampsUnsupportedMode(t *testing.T) {
	s := &Session{
		mode:  "auto",
		model: "auto-model",
		models: []claude.ModelInfo{
			{Value: "auto-model", SupportsAutoMode: true},
			{Value: "plain-model", SupportsAutoMode: false},
		},
	}
	if got := s.setModel("plain-model"); got != "manual" {
		t.Fatalf("clamped mode = %q, want manual", got)
	}
	if s.mode != "manual" {
		t.Fatalf("session mode = %q, want manual", s.mode)
	}
	if s.hasMode("auto") {
		t.Fatal("auto still advertised after switching to a model without it")
	}
}

func TestSetModelKeepsSupportedMode(t *testing.T) {
	s := &Session{
		mode:  "auto",
		model: "auto-a",
		models: []claude.ModelInfo{
			{Value: "auto-a", SupportsAutoMode: true},
			{Value: "auto-b", SupportsAutoMode: true},
		},
	}
	if got := s.setModel("auto-b"); got != "" {
		t.Fatalf("mode unexpectedly clamped to %q", got)
	}
	if s.mode != "auto" {
		t.Fatalf("session mode = %q, want auto", s.mode)
	}
}

func TestBypassAllowed(t *testing.T) {
	cases := []struct {
		euid    int
		sandbox bool
		want    bool
	}{
		{0, false, false},
		{0, true, true},
		{501, false, true},
	}
	for _, tc := range cases {
		if got := bypassAllowed(tc.euid, tc.sandbox); got != tc.want {
			t.Fatalf("bypassAllowed(%d, %v) = %v, want %v", tc.euid, tc.sandbox, got, tc.want)
		}
	}
}

func TestSetSessionModeRejectsUnavailableMode(t *testing.T) {
	sess := &Session{
		id:     "s1",
		mode:   "manual",
		model:  "plain-model",
		models: []claude.ModelInfo{{Value: "plain-model", SupportsAutoMode: false}},
	}
	a := &Agent{sessions: map[string]*Session{"s1": sess}}

	_, rerr := a.SetSessionMode(context.Background(), acp.SetSessionModeRequest{
		SessionID: "s1",
		ModeID:    "auto",
	})
	if rerr == nil || rerr.Code != -32602 {
		t.Fatalf("err = %v, want -32602 for an unavailable mode", rerr)
	}

	_, rerr = a.SetSessionMode(context.Background(), acp.SetSessionModeRequest{
		SessionID: "s1",
		ModeID:    "bogus",
	})
	if rerr == nil || rerr.Code != -32602 {
		t.Fatalf("err = %v, want -32602 for an unknown mode", rerr)
	}
}
