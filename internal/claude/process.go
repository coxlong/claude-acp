package claude

import (
	"bufio"
	"bytes"
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
	"sync/atomic"
	"time"
)

const (
	maxLineBytes = 32 << 20
	// maxQueuedEvents bounds the unbounded-style event queue. CLI output per
	// turn is bounded by the model's output budget, so this is a pathological
	// case only; hitting it marks the client dropped instead of silently
	// discarding messages (a dropped ResultMessage would hang the prompt).
	maxQueuedEvents = 100_000
	stderrLimit     = 64 << 10
)

// eventQueue is an unbounded FIFO of CLI messages. Consumers read through a
// channel fed by a pump goroutine; producers never block and never silently
// drop. capacity 0 means unbounded (items still capped by maxQueuedEvents via
// the caller checking EmitDropped).
type eventQueue struct {
	mu     sync.Mutex
	cond   *sync.Cond
	items  []Message
	closed bool
}

func newEventQueue() *eventQueue {
	q := &eventQueue{}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *eventQueue) push(m Message) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return false
	}
	q.items = append(q.items, m)
	q.cond.Signal()
	return true
}

// pop blocks until a message is available or the queue is closed. ok is false
// once the queue is closed and drained.
func (q *eventQueue) pop() (Message, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.items) == 0 && !q.closed {
		q.cond.Wait()
	}
	if len(q.items) == 0 {
		return nil, false
	}
	m := q.items[0]
	q.items[0] = nil
	q.items = q.items[1:]
	return m, true
}

func (q *eventQueue) close() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	q.cond.Broadcast()
}

func (q *eventQueue) size() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

// Client is a running Claude Code CLI process speaking stream-json.
type Client struct {
	opts Options
	log  *slog.Logger

	cmd   *exec.Cmd
	stdin io.WriteCloser

	writeMu sync.Mutex

	events    *eventQueue
	eventCh   chan Message
	dropped   atomic.Bool
	done      chan struct{}
	doneOnce  sync.Once
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
		events:  newEventQueue(),
		eventCh: make(chan Message, 256),
		done:    make(chan struct{}),
		pending: make(map[string]chan controlResult),
	}

	// Pump from the unbounded queue into the public channel. The pump blocks
	// on send when the consumer is slow; the queue absorbs the difference so
	// no event is ever dropped.
	go func() {
		defer close(c.eventCh)
		for {
			m, ok := c.events.pop()
			if !ok {
				return
			}
			c.eventCh <- m
		}
	}()

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
func (c *Client) Events() <-chan Message { return c.eventCh }

// EmitDropped reports whether messages were discarded because the event queue
// exceeded maxQueuedEvents. A prompt that observes it cannot trust the event
// stream and must terminate the session.
func (c *Client) EmitDropped() bool { return c.dropped.Load() }

// Done is closed when the client terminates, including an unexpected CLI
// exit (not only Close).
func (c *Client) Done() <-chan struct{} { return c.done }

func (c *Client) markDone() {
	c.doneOnce.Do(func() { close(c.done) })
}

// StderrTail returns the last stderr output, for diagnostics.
func (c *Client) StderrTail() string {
	c.stderrMu.Lock()
	defer c.stderrMu.Unlock()
	return c.stderrTail.String()
}

// Close terminates the process.
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		c.markDone()
		c.events.close()
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
	defer func() {
		c.markDone()
		c.events.close()
	}()

	// Incremental, size-capped reading. bufio.ReadBytes would accumulate a
	// newline-less flood without bound (the cap would apply only after the
	// whole payload was in memory).
	r := bufio.NewReaderSize(stdout, 1<<20)
	buf := make([]byte, 64<<10)
	var acc []byte
	for {
		n, err := r.Read(buf)
		if n > 0 {
			acc = append(acc, buf[:n]...)
			for {
				i := bytes.IndexByte(acc, '\n')
				if i < 0 {
					break
				}
				line := acc[:i]
				acc = acc[i+1:]
				if len(line) > 0 {
					c.handleLine(line)
				}
			}
			if len(acc) > maxLineBytes {
				c.log.Warn("claude: line exceeds limit", "bytes", len(acc))
				return
			}
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
	if c.events.size() >= maxQueuedEvents {
		if !c.dropped.Swap(true) {
			c.log.Warn("claude: event queue exceeded limit; marking stream untrusted")
		}
		return
	}
	c.events.push(msg)
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
