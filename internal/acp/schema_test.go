package acp

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// compileDef builds a small schema document that references one definition of
// the vendored official ACP schema.
func compileDef(t *testing.T, name string) *jsonschema.Schema {
	t.Helper()
	data, err := os.ReadFile("../../schema/schema.json")
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("parse schema: %v", err)
	}
	root, ok := doc.(map[string]any)
	if !ok {
		t.Fatal("schema root is not an object")
	}
	defs, ok := root["$defs"]
	if !ok {
		t.Fatal("schema has no $defs")
	}
	wrapper := map[string]any{
		"$schema": "https://json-schema.org/draft/2020-12/schema",
		"$ref":    "#/$defs/" + name,
		"$defs":   defs,
	}
	c := jsonschema.NewCompiler()
	if err := c.AddResource("acp.json", wrapper); err != nil {
		t.Fatalf("add schema: %v", err)
	}
	sch, err := c.Compile("acp.json")
	if err != nil {
		t.Fatalf("compile %s: %v", name, err)
	}
	return sch
}

func validate(t *testing.T, def string, v any) {
	t.Helper()
	sch := compileDef(t, def)
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var instance any
	if err := json.Unmarshal(b, &instance); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if err := sch.Validate(instance); err != nil {
		t.Fatalf("%s rejected %s:\n%v", def, b, err)
	}
}

func TestInitializeResponseMatchesSchema(t *testing.T) {
	resp := InitializeResponse{
		ProtocolVersion: ProtocolVersion,
		AgentCapabilities: AgentCapabilities{
			LoadSession:        true,
			PromptCapabilities: PromptCapabilities{},
			McpCapabilities:    McpCapabilities{HTTP: true, SSE: true},
			SessionCapabilities: &SessionCapabilities{
				List:  &SessionListCapabilities{},
				Close: &SessionCloseCapabilities{},
			},
		},
		AuthMethods: []AuthMethod{},
	}
	validate(t, "InitializeResponse", resp)
}

func TestNewSessionResponseMatchesSchema(t *testing.T) {
	resp := NewSessionResponse{
		SessionID: "00000000-0000-4000-8000-000000000000",
		Modes: &SessionModeState{
			CurrentModeID:  "default",
			AvailableModes: []SessionMode{{ID: "default", Name: "Default"}},
		},
		ConfigOptions: []SessionConfigOption{
			{
				ID:           "model",
				Name:         "Model",
				Category:     "model",
				Type:         "select",
				CurrentValue: "default",
				Options:      []SessionConfigSelectOption{{Value: "default", Name: "Default"}},
			},
		},
	}
	validate(t, "NewSessionResponse", resp)
}

func TestSessionUpdateMatchesSchema(t *testing.T) {
	cases := []SessionNotification{
		{
			SessionID: "s1",
			Update: SessionUpdate{
				SessionUpdate: UpdateAgentMessageChunk,
				Content:       TextContent("hello"),
			},
		},
		{
			SessionID: "s1",
			Update: SessionUpdate{
				SessionUpdate: UpdateToolCall,
				ToolCallID:    "tool-1",
				Title:         "Read /tmp/a",
				Kind:          ToolKindRead,
				Status:        ToolStatusPending,
			},
		},
		{
			SessionID: "s1",
			Update: SessionUpdate{
				SessionUpdate: UpdateToolCallUpdate,
				ToolCallID:    "tool-1",
				Status:        ToolStatusCompleted,
			},
		},
		{
			SessionID: "s1",
			Update: SessionUpdate{
				SessionUpdate: UpdateCurrentMode,
				CurrentModeID: "plan",
			},
		},
	}
	for _, c := range cases {
		validate(t, "SessionNotification", c)
	}
}

func TestListSessionsResponseMatchesSchema(t *testing.T) {
	validate(t, "ListSessionsResponse", ListSessionsResponse{Sessions: []SessionInfo{}})

	resp := ListSessionsResponse{
		Sessions: []SessionInfo{{
			SessionID: "00000000-0000-4000-8000-000000000000",
			Cwd:       "/tmp/project",
			Title:     "Fix the build",
			UpdatedAt: "2026-10-18T00:00:00Z",
		}},
	}
	validate(t, "ListSessionsResponse", resp)
}

func TestRequestPermissionMatchesSchema(t *testing.T) {
	req := RequestPermissionRequest{
		SessionID: "s1",
		ToolCall: ToolCall{
			ToolCallID: "tool-1",
			Title:      "Write /tmp/x",
			Kind:       ToolKindEdit,
			Status:     ToolStatusPending,
		},
		Options: []PermissionOption{
			{OptionID: "allow", Name: "Allow Once", Kind: PermissionAllowOnce},
			{OptionID: "reject", Name: "Deny", Kind: PermissionRejectOnce},
		},
	}
	validate(t, "RequestPermissionRequest", req)
}
