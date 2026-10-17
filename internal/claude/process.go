package claude

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

const (
	maxLineBytes = 32 << 20
	eventBuffer  = 4096
	stderrLimit  = 64 << 10
)

// Client is a running Claude Code CLI process speaking stream-json.
type Client struct {
	opts Options
	log  *slog.Logger

	cmd   *exec.Cmd
	stdin io.WriteCloser

	writeMu sync.Mutex

	events    chan Message
	done      chan struct{}
	closeOnce sync.Once

	pendingMu sync.Mutex
	pending   map[string]chan controlResult

	initMu     sync.Mutex
	initResult map[string]any

	stderrMu   sync.Mutex
	stderrTail strings.Builder
}

// Start launches the CLI and performs the initialize handshake.
func Start(ctx context.Context, opts Options) (*Client, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	if opts.Log == nil {
		opts.Log = slog.Default()
	}
	path := opts.CliPath
	if path == "" {
		path = "claude"
	}

	cmd := exec.Command(path, BuildArgs(opts)...)
	cmd.Dir = opts.Cwd
	cmd.Env = buildEnv(opts.Env)
	setProcAttr(cmd)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("stdin pipe: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("stdout pipe: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("stderr pipe: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start claude: %w", err)
	}

	c := &Client{
		opts:    opts,
		log:     opts.Log,
		cmd:     cmd,
		stdin:   stdin,
		events:  make(chan Message, eventBuffer),
		done:    make(chan struct{}),
		pending: make(map[string]chan controlResult),
	}

	go c.readLoop(stdout)
	go c.readStderr(stderr)

	initCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	if err := c.initialize(initCtx); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("initialize claude: %w", err)
	}
	return c, nil
}

// Events yields non-control messages until the process exits.
func (c *Client) Events() <-chan Message { return c.events }

// Done is closed when the client terminates.
func (c *Client) Done() <-chan struct{} { return c.done }

// StderrTail returns the last stderr output, for diagnostics.
func (c *Client) StderrTail() string {
	c.stderrMu.Lock()
	defer c.stderrMu.Unlock()
	return c.stderrTail.String()
}

// Close terminates the process.
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		close(c.done)
		_ = c.stdin.Close()

		waited := make(chan struct{})
		go func() {
			_ = c.cmd.Wait()
			close(waited)
		}()
		select {
		case <-waited:
		case <-time.After(5 * time.Second):
			killProcessGroup(c.cmd)
			<-waited
		}
	})
	return nil
}

func (c *Client) readLoop(stdout io.ReadCloser) {
	defer close(c.events)
	r := bufio.NewReaderSize(stdout, 1<<20)
	for {
		line, err := r.ReadBytes('\n')
		if len(line) > maxLineBytes {
			c.log.Warn("claude: line exceeds limit", "bytes", len(line))
			return
		}
		if len(line) > 0 {
			c.handleLine(line)
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				c.log.Debug("claude: stdout read failed", "err", err)
			}
			return
		}
	}
}

func (c *Client) readStderr(stderr io.ReadCloser) {
	r := bufio.NewReaderSize(stderr, 64<<10)
	for {
		line, err := r.ReadString('\n')
		if line != "" {
			trimmed := strings.TrimRight(line, "\r\n")
			c.stderrMu.Lock()
			c.stderrTail.WriteString(trimmed)
			c.stderrTail.WriteByte('\n')
			if c.stderrTail.Len() > stderrLimit {
				s := c.stderrTail.String()
				c.stderrTail.Reset()
				c.stderrTail.WriteString(s[len(s)-stderrLimit:])
			}
			c.stderrMu.Unlock()
			if c.opts.OnStderr != nil {
				c.opts.OnStderr(trimmed)
			}
		}
		if err != nil {
			return
		}
	}
}

func (c *Client) handleLine(line []byte) {
	var kind struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(line, &kind); err != nil {
		c.log.Debug("claude: malformed line", "err", err)
		return
	}

	switch kind.Type {
	case "control_response":
		var env controlResponse
		if err := json.Unmarshal(line, &env); err != nil {
			c.log.Warn("claude: malformed control_response", "err", err)
			return
		}
		c.deliverControl(env)
	case "control_request":
		var env controlRequest
		if err := json.Unmarshal(line, &env); err != nil {
			c.log.Warn("claude: malformed control_request", "err", err)
			return
		}
		go c.handleControlRequest(env)
	case "keep_alive":
		c.emit(&KeepAlive{})
	case "system":
		var raw map[string]any
		_ = json.Unmarshal(line, &raw)
		c.emit(&SystemMessage{
			Subtype:        asString(raw["subtype"]),
			SessionID:      asString(raw["session_id"]),
			Model:          asString(raw["model"]),
			PermissionMode: asString(raw["permissionMode"]),
			Raw:            raw,
		})
	case "assistant":
		c.emit(parseAssistant(line))
	case "user":
		c.emit(parseUser(line))
	case "stream_event":
		var env struct {
			Event           map[string]any `json:"event"`
			ParentToolUseID string         `json:"parent_tool_use_id"`
		}
		if err := json.Unmarshal(line, &env); err == nil {
			c.emit(&StreamEvent{Event: env.Event, ParentToolUseID: env.ParentToolUseID})
		}
	case "result":
		var msg ResultMessage
		if err := json.Unmarshal(line, &msg); err == nil {
			c.emit(&msg)
		}
	default:
		c.log.Debug("claude: ignoring message type", "type", kind.Type)
	}
}

func (c *Client) emit(msg Message) {
	select {
	case c.events <- msg:
	case <-c.done:
	default:
		c.log.Warn("claude: event buffer full, dropping message")
	}
}

func (c *Client) writeJSON(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b = append(b, '\n')

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_, err = c.stdin.Write(b)
	return err
}

// SendUserText sends a text-only prompt to the CLI.
func (c *Client) SendUserText(text string) error {
	return c.writeJSON(map[string]any{
		"type":       "user",
		"session_id": "",
		"message": map[string]any{
			"role":    "user",
			"content": []any{map[string]any{"type": "text", "text": text}},
		},
		"parent_tool_use_id": nil,
	})
}

func parseAssistant(line []byte) Message {
	var w struct {
		Message struct {
			ID         string         `json:"id"`
			Content    []ContentBlock `json:"content"`
			Model      string         `json:"model"`
			StopReason string         `json:"stop_reason"`
		} `json:"message"`
		ParentToolUseID string `json:"parent_tool_use_id"`
		UUID            string `json:"uuid"`
		Error           string `json:"error"`
	}
	if err := json.Unmarshal(line, &w); err != nil {
		return nil
	}
	return &AssistantMessage{
		MessageID:       w.Message.ID,
		Content:         w.Message.Content,
		Model:           w.Message.Model,
		StopReason:      w.Message.StopReason,
		ParentToolUseID: w.ParentToolUseID,
		UUID:            w.UUID,
		Error:           w.Error,
	}
}

func parseUser(line []byte) Message {
	var w struct {
		Message struct {
			Content []ContentBlock `json:"content"`
		} `json:"message"`
		ParentToolUseID string `json:"parent_tool_use_id"`
		IsMeta          bool   `json:"isMeta"`
		UUID            string `json:"uuid"`
	}
	if err := json.Unmarshal(line, &w); err != nil {
		return nil
	}
	return &UserMessage{
		Content:         w.Message.Content,
		ParentToolUseID: w.ParentToolUseID,
		IsMeta:          w.IsMeta,
		UUID:            w.UUID,
	}
}

func buildEnv(extra map[string]string) []string {
	merged := make(map[string]string)
	for _, kv := range os.Environ() {
		if i := strings.IndexByte(kv, '='); i > 0 {
			merged[kv[:i]] = kv[i+1:]
		}
	}
	delete(merged, "NODE_OPTIONS")
	for k, v := range extra {
		merged[k] = v
	}
	out := make([]string, 0, len(merged))
	for k, v := range merged {
		out = append(out, k+"="+v)
	}
	return out
}

func asString(v any) string {
	s, _ := v.(string)
	return s
}
