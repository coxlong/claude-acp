// Package acp implements the Agent Client Protocol (protocol version 1) for
// the subset of methods this agent supports.
package acp

// ProtocolVersion is the ACP protocol version implemented here.
const ProtocolVersion = 1

// Agent methods (client -> agent).
const (
	MethodInitialize       = "initialize"
	MethodAuthenticate     = "authenticate"
	MethodLogout           = "logout"
	MethodSessionNew       = "session/new"
	MethodSessionPrompt    = "session/prompt"
	MethodSessionCancel    = "session/cancel"
	MethodSessionLoad      = "session/load"
	MethodSessionList      = "session/list"
	MethodSessionClose     = "session/close"
	MethodSessionSetMode   = "session/set_mode"
	MethodSessionSetConfig = "session/set_config_option"
	MethodSessionResume    = "session/resume"
	MethodSessionFork      = "session/fork"
	MethodSessionDelete    = "session/delete"
	MethodCancelRequest    = "$/cancel_request"
)

// Client methods (agent -> client).
const (
	ClientSessionUpdate     = "session/update"
	ClientRequestPermission = "session/request_permission"
)

// Stop reasons returned by session/prompt.
const (
	StopReasonEndTurn         = "end_turn"
	StopReasonMaxTokens       = "max_tokens"
	StopReasonMaxTurnRequests = "max_turn_requests"
	StopReasonRefusal         = "refusal"
	StopReasonCancelled       = "cancelled"
)

// Session update kinds.
const (
	UpdateAgentMessageChunk = "agent_message_chunk"
	UpdateAgentThoughtChunk = "agent_thought_chunk"
	UpdateUserMessageChunk  = "user_message_chunk"
	UpdateToolCall          = "tool_call"
	UpdateToolCallUpdate    = "tool_call_update"
	UpdateCurrentMode       = "current_mode_update"
	UpdateConfigOptions     = "config_option_update"
)

// Tool call kinds.
const (
	ToolKindRead    = "read"
	ToolKindEdit    = "edit"
	ToolKindDelete  = "delete"
	ToolKindMove    = "move"
	ToolKindSearch  = "search"
	ToolKindExecute = "execute"
	ToolKindThink   = "think"
	ToolKindFetch   = "fetch"
	ToolKindOther   = "other"
)

// Tool call statuses.
const (
	ToolStatusPending    = "pending"
	ToolStatusInProgress = "in_progress"
	ToolStatusCompleted  = "completed"
	ToolStatusFailed     = "failed"
)

// Permission option kinds.
const (
	PermissionAllowOnce    = "allow_once"
	PermissionAllowAlways  = "allow_always"
	PermissionRejectOnce   = "reject_once"
	PermissionRejectAlways = "reject_always"
)
