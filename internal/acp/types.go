package acp

// ---- Initialize ----

type ClientInfo struct {
	Name    string `json:"name"`
	Title   string `json:"title,omitempty"`
	Version string `json:"version,omitempty"`
}

type ClientCapabilities struct {
	FS          any `json:"fs,omitempty"`
	Terminal    any `json:"terminal,omitempty"`
	Elicitation any `json:"elicitation,omitempty"`
	Meta        any `json:"_meta,omitempty"`
}

type InitializeRequest struct {
	ProtocolVersion    int                `json:"protocolVersion"`
	ClientCapabilities ClientCapabilities `json:"clientCapabilities"`
	ClientInfo         *ClientInfo        `json:"clientInfo,omitempty"`
}

type PromptCapabilities struct {
	Image           bool `json:"image"`
	Audio           bool `json:"audio"`
	EmbeddedContext bool `json:"embeddedContext"`
}

type McpCapabilities struct {
	HTTP bool `json:"http"`
	SSE  bool `json:"sse"`
}

type SessionListCapabilities struct{}
type SessionCloseCapabilities struct{}

type SessionCapabilities struct {
	List  *SessionListCapabilities  `json:"list,omitempty"`
	Close *SessionCloseCapabilities `json:"close,omitempty"`
}

type AgentCapabilities struct {
	LoadSession         bool                 `json:"loadSession"`
	PromptCapabilities  PromptCapabilities   `json:"promptCapabilities"`
	McpCapabilities     McpCapabilities      `json:"mcpCapabilities"`
	SessionCapabilities *SessionCapabilities `json:"sessionCapabilities,omitempty"`
}

type AuthMethod struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
}

type InitializeResponse struct {
	ProtocolVersion   int               `json:"protocolVersion"`
	AgentCapabilities AgentCapabilities `json:"agentCapabilities"`
	AuthMethods       []AuthMethod      `json:"authMethods"`
}

// ---- Authenticate ----

type AuthenticateRequest struct {
	MethodID string `json:"methodId"`
}

type AuthenticateResponse struct{}

// ---- Sessions ----

// EnvVariable is one environment variable entry of a stdio MCP server.
type EnvVariable struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// HttpHeader is one header entry of an HTTP/SSE MCP server.
type HttpHeader struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type McpServer struct {
	Name    string        `json:"name"`
	Command string        `json:"command,omitempty"`
	Args    []string      `json:"args,omitempty"`
	Env     []EnvVariable `json:"env,omitempty"`
	Type    string        `json:"type,omitempty"`
	URL     string        `json:"url,omitempty"`
	Headers []HttpHeader  `json:"headers,omitempty"`
}

type NewSessionRequest struct {
	Cwd                   string         `json:"cwd"`
	McpServers            []McpServer    `json:"mcpServers"`
	AdditionalDirectories []string       `json:"additionalDirectories,omitempty"`
	Meta                  map[string]any `json:"_meta,omitempty"`
}

type SessionMode struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
}

type SessionModeState struct {
	CurrentModeID  string        `json:"currentModeId"`
	AvailableModes []SessionMode `json:"availableModes"`
}

type SessionConfigSelectOption struct {
	Value       string `json:"value"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
}

type SessionConfigOption struct {
	ID           string                      `json:"id"`
	Name         string                      `json:"name"`
	Description  string                      `json:"description,omitempty"`
	Category     string                      `json:"category,omitempty"`
	Type         string                      `json:"type"`
	CurrentValue any                         `json:"currentValue"`
	Options      []SessionConfigSelectOption `json:"options"`
}

type NewSessionResponse struct {
	SessionID     string                `json:"sessionId"`
	Modes         *SessionModeState     `json:"modes,omitempty"`
	ConfigOptions []SessionConfigOption `json:"configOptions,omitempty"`
}

type LoadSessionRequest struct {
	SessionID  string      `json:"sessionId"`
	Cwd        string      `json:"cwd"`
	McpServers []McpServer `json:"mcpServers"`
}

type LoadSessionResponse struct {
	Modes         *SessionModeState     `json:"modes,omitempty"`
	ConfigOptions []SessionConfigOption `json:"configOptions,omitempty"`
}

type ListSessionsRequest struct {
	Cwd string `json:"cwd,omitempty"`
}

type SessionInfo struct {
	SessionID string `json:"sessionId"`
	Cwd       string `json:"cwd"`
	Title     string `json:"title,omitempty"`
	UpdatedAt string `json:"updatedAt,omitempty"`
}

type ListSessionsResponse struct {
	Sessions []SessionInfo `json:"sessions"`
}

type CloseSessionRequest struct {
	SessionID string `json:"sessionId"`
}

type CloseSessionResponse struct{}

type SetSessionModeRequest struct {
	SessionID string `json:"sessionId"`
	ModeID    string `json:"modeId"`
}

type SetSessionModeResponse struct{}

type SetSessionConfigOptionRequest struct {
	SessionID string `json:"sessionId"`
	ConfigID  string `json:"configId"`
	Value     any    `json:"value"`
}

type SetSessionConfigOptionResponse struct {
	ConfigOptions []SessionConfigOption `json:"configOptions,omitempty"`
}

// ---- Prompt ----

type ContentBlock struct {
	Type string `json:"type"`
	Text string `json:"text,omitempty"`
}

type PromptRequest struct {
	SessionID string         `json:"sessionId"`
	Prompt    []ContentBlock `json:"prompt"`
}

type PromptResponse struct {
	StopReason string `json:"stopReason"`
}

type CancelNotification struct {
	SessionID string `json:"sessionId"`
}

// ---- Agent -> client ----

type SessionNotification struct {
	SessionID string        `json:"sessionId"`
	Update    SessionUpdate `json:"update"`
}

// SessionUpdate is a union discriminated by the sessionUpdate field. Content
// is either a text content block (message/thought chunks) or a list of tool
// call content entries (tool_call_update).
type SessionUpdate struct {
	SessionUpdate string `json:"sessionUpdate"`

	Content any `json:"content,omitempty"`

	ToolCallID string `json:"toolCallId,omitempty"`
	Title      string `json:"title,omitempty"`
	Kind       string `json:"kind,omitempty"`
	Status     string `json:"status,omitempty"`
	RawInput   any    `json:"rawInput,omitempty"`

	CurrentModeID string                `json:"currentModeId,omitempty"`
	ConfigOptions []SessionConfigOption `json:"configOptions,omitempty"`
}

type PermissionOption struct {
	OptionID string `json:"optionId"`
	Name     string `json:"name"`
	Kind     string `json:"kind"`
}

type ToolCall struct {
	ToolCallID string `json:"toolCallId"`
	Title      string `json:"title,omitempty"`
	Kind       string `json:"kind,omitempty"`
	Status     string `json:"status,omitempty"`
	Content    any    `json:"content,omitempty"`
	RawInput   any    `json:"rawInput,omitempty"`
}

type RequestPermissionRequest struct {
	SessionID string             `json:"sessionId"`
	ToolCall  ToolCall           `json:"toolCall"`
	Options   []PermissionOption `json:"options"`
}

type PermissionOutcome struct {
	Outcome  string `json:"outcome"` // "selected" or "cancelled"
	OptionID string `json:"optionId,omitempty"`
}

type RequestPermissionResponse struct {
	Outcome PermissionOutcome `json:"outcome"`
}

// TextContent builds a text content block for session updates.
func TextContent(text string) map[string]any {
	return map[string]any{"type": "text", "text": text}
}
