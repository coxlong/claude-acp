// Package claude speaks the Claude Code CLI stream-json protocol and its
// bidirectional control protocol.
package claude

import "encoding/json"

// Message is one line of CLI stdout (excluding control traffic).
type Message interface{ isMessage() }

// SystemMessage covers system/init and related subtypes.
type SystemMessage struct {
	Subtype        string
	SessionID      string
	Model          string
	PermissionMode string
	Raw            map[string]any
}

func (*SystemMessage) isMessage() {}

// AssistantMessage is a complete assistant message. Content includes text,
// thinking and tool_use blocks.
type AssistantMessage struct {
	MessageID       string
	Content         []ContentBlock
	Model           string
	StopReason      string
	ParentToolUseID string
	UUID            string
	Error           string
}

func (*AssistantMessage) isMessage() {}

// UserMessage carries tool results (and, with --replay-user-messages, echoes
// of user prompts).
type UserMessage struct {
	Content         []ContentBlock
	ParentToolUseID string
	IsMeta          bool
	UUID            string
}

func (*UserMessage) isMessage() {}

// StreamEvent carries a partial streaming event (message_start,
// content_block_delta, ...).
type StreamEvent struct {
	Event           map[string]any
	ParentToolUseID string
}

func (*StreamEvent) isMessage() {}

// ResultMessage ends a turn.
type ResultMessage struct {
	Subtype        string         `json:"subtype"`
	IsError        bool           `json:"is_error"`
	Result         string         `json:"result"`
	StopReason     string         `json:"stop_reason"`
	TerminalReason string         `json:"terminal_reason"`
	Errors         []any          `json:"errors"`
	SessionID      string         `json:"session_id"`
	NumTurns       int            `json:"num_turns"`
	Usage          map[string]any `json:"usage"`
}

func (*ResultMessage) isMessage() {}

// KeepAlive is emitted by the CLI to keep the stream warm.
type KeepAlive struct{}

func (*KeepAlive) isMessage() {}

// ContentBlock is one entry of a message's content array.
type ContentBlock struct {
	Type      string         `json:"type"`
	Text      string         `json:"text,omitempty"`
	Thinking  string         `json:"thinking,omitempty"`
	ID        string         `json:"id,omitempty"`
	Name      string         `json:"name,omitempty"`
	Input     map[string]any `json:"input,omitempty"`
	ToolUseID string         `json:"tool_use_id,omitempty"`
	Content   any            `json:"content,omitempty"`
	IsError   bool           `json:"is_error,omitempty"`
}

// PermissionRequest is the payload of an inbound can_use_tool control
// request.
type PermissionRequest struct {
	ToolName       string
	Input          map[string]any
	ToolUseID      string
	Suggestions    []map[string]any
	DecisionReason string
	BlockedPath    string
	Title          string
	DisplayName    string
	Description    string
	AgentID        string
	RequestID      string
}

// PermissionResult is returned by the CanUseTool callback.
type PermissionResult struct {
	Behavior           string // "allow" or "deny"
	UpdatedInput       map[string]any
	UpdatedPermissions []map[string]any
	Message            string
	Interrupt          bool
}

// Allow returns an allow result.
func Allow(updatedInput map[string]any) PermissionResult {
	return PermissionResult{Behavior: "allow", UpdatedInput: updatedInput}
}

// Deny returns a deny result.
func Deny(message string) PermissionResult {
	return PermissionResult{Behavior: "deny", Message: message}
}

// ModelInfo is one entry of the CLI initialization result's models array.
type ModelInfo struct {
	Value                 string   `json:"value"`
	ModelID               string   `json:"modelId"`
	DisplayName           string   `json:"displayName"`
	Description           string   `json:"description"`
	SupportsEffort        bool     `json:"supportsEffort"`
	SupportedEffortLevels []string `json:"supportedEffortLevels"`
	SupportsAutoMode      bool     `json:"supportsAutoMode"`
	ResolvedModel         string   `json:"resolvedModel"`
}

// Name returns the model's stable identifier.
func (m ModelInfo) Name() string {
	if m.Value != "" {
		return m.Value
	}
	return m.ModelID
}

// controlRequest is the envelope of a CLI -> agent control request.
type controlRequest struct {
	Type      string `json:"type"`
	RequestID string `json:"request_id"`
	Request   struct {
		Subtype               string           `json:"subtype"`
		ToolName              string           `json:"tool_name"`
		Input                 map[string]any   `json:"input"`
		ToolUseID             string           `json:"tool_use_id"`
		PermissionSuggestions []map[string]any `json:"permission_suggestions"`
		DecisionReason        string           `json:"decision_reason"`
		BlockedPath           string           `json:"blocked_path"`
		Title                 string           `json:"title"`
		DisplayName           string           `json:"display_name"`
		Description           string           `json:"description"`
		AgentID               string           `json:"agent_id"`
	} `json:"request"`
}

// controlResponse is the envelope of a CLI -> agent control response.
type controlResponse struct {
	Type     string `json:"type"`
	Response struct {
		Subtype   string          `json:"subtype"`
		RequestID string          `json:"request_id"`
		Response  json.RawMessage `json:"response"`
		Error     string          `json:"error"`
	} `json:"response"`
}

func marshal(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}
