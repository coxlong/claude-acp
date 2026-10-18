package agent

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/coxlong/claude-acp/internal/acp"
	"github.com/coxlong/claude-acp/internal/claude"
	"github.com/coxlong/claude-acp/internal/rpc"
)

// turnStallTimeout bounds how long a prompt waits for any CLI output. Without
// it a wedged CLI (or a silently dropped event) would hold promptMu forever.
const turnStallTimeout = 10 * time.Minute

// Prompt implements acp.Handler.
func (a *Agent) Prompt(ctx context.Context, req acp.PromptRequest) (acp.PromptResponse, *rpc.Error) {
	sess := a.getSession(req.SessionID)
	if sess == nil {
		return acp.PromptResponse{}, rpc.ErrInvalidParams("unknown session: " + req.SessionID)
	}
	text := promptText(req.Prompt)
	if text == "" {
		return acp.PromptResponse{}, rpc.ErrInvalidParams("prompt contains no text")
	}

	// Register the dispatch so a cancel arriving before the turn starts is
	// applied to it instead of being silently dropped.
	a.markDispatch(req.SessionID)
	defer a.clearDispatch(req.SessionID)

	if !sess.promptMu.TryLock() {
		return acp.PromptResponse{}, rpc.ErrInvalidParams("session is busy")
	}
	defer sess.promptMu.Unlock()

	if sess.beginTurn(ctx) {
		// A session/cancel raced the prompt dispatch. Release the turn
		// state beginTurn created, or running stays stuck on the idle
		// session and the turn context leaks.
		sess.endTurn()
		return acp.PromptResponse{StopReason: acp.StopReasonCancelled}, nil
	}
	defer sess.endTurn()

	if err := sess.client.SendUserText(text); err != nil {
		return acp.PromptResponse{}, rpc.ErrInternalMsg(err.Error())
	}

	turnCtx := sess.turnContext()
	watchdog := time.NewTimer(turnStallTimeout)
	defer watchdog.Stop()
	for {
		select {
		case msg, ok := <-sess.client.Events():
			if !ok {
				a.dropSession(sess, "process exited during prompt")
				return acp.PromptResponse{}, rpc.ErrInternalMsg(a.exitError(sess))
			}
			if !watchdog.Stop() {
				select {
				case <-watchdog.C:
				default:
				}
			}
			watchdog.Reset(turnStallTimeout)
			if sess.client.EmitDropped() {
				a.dropSession(sess, "claude event queue overflow")
				return acp.PromptResponse{}, rpc.ErrInternalMsg("claude event queue overflow; session terminated")
			}
			done, stop, rerr := a.handleEvent(sess, msg)
			if rerr != nil {
				return acp.PromptResponse{}, rerr
			}
			if done {
				return acp.PromptResponse{StopReason: stop}, nil
			}
		case <-watchdog.C:
			a.dropSession(sess, "claude output stalled")
			return acp.PromptResponse{}, rpc.ErrInternalMsg("claude produced no output for " + turnStallTimeout.String() + "; session terminated")
		case <-turnCtx.Done():
			// Either session/cancel cancelled the turn context, or the
			// JSON-RPC request itself was cancelled ($/cancel_request).
			_ = sess.client.Interrupt(context.Background())
			if !drainTurn(sess) {
				// The CLI did not settle in time; kill it so stale events
				// cannot shift the next turn's correlation.
				a.dropSession(sess, "turn did not drain after cancel")
			}
			if ctx.Err() != nil {
				// The prompt request itself was cancelled (-32800).
				return acp.PromptResponse{}, rpc.ErrCancelled
			}
			// session/cancel: the prompt answers with a stop reason.
			return acp.PromptResponse{StopReason: acp.StopReasonCancelled}, nil
		}
	}
}

func (a *Agent) handleEvent(sess *Session, msg claude.Message) (done bool, stop string, rerr *rpc.Error) {
	// Subagent messages carry ParentToolUseID, but this agent does not
	// advertise the subagent-transcript capability; the CLI's legacy
	// flattened mode is forwarded as-is so tool results always pair with the
	// permission requests that announce them.
	switch m := msg.(type) {
	case *claude.StreamEvent:
		a.handleStreamEvent(sess, m)
	case *claude.AssistantMessage:
		a.handleAssistant(sess, m)
	case *claude.UserMessage:
		a.handleUser(sess, m)
	case *claude.ResultMessage:
		stop := a.resultStopReason(sess, m)
		// Turns that end with a valid ACP stop reason are not RPC errors.
		switch stop {
		case acp.StopReasonCancelled, acp.StopReasonMaxTurnRequests,
			acp.StopReasonMaxTokens, acp.StopReasonRefusal:
			return true, stop, nil
		}
		return true, stop, a.resultError(m)
	case *claude.SystemMessage, *claude.KeepAlive:
		// Session metadata and keep-alives need no ACP translation in P1.
	}
	return false, "", nil
}

func (a *Agent) handleStreamEvent(sess *Session, m *claude.StreamEvent) {
	if asString(m.Event["type"]) != "content_block_delta" {
		return
	}
	delta, ok := m.Event["delta"].(map[string]any)
	if !ok {
		return
	}
	switch asString(delta["type"]) {
	case "text_delta":
		text := asString(delta["text"])
		if text == "" {
			return
		}
		sess.markTextDelta()
		a.notify(sess, acp.SessionUpdate{
			SessionUpdate: acp.UpdateAgentMessageChunk,
			Content:       acp.TextContent(text),
		})
	case "thinking_delta":
		text := asString(delta["thinking"])
		if text == "" {
			return
		}
		sess.markThinkingDelta()
		a.notify(sess, acp.SessionUpdate{
			SessionUpdate: acp.UpdateAgentThoughtChunk,
			Content:       acp.TextContent(text),
		})
	}
}

func (a *Agent) handleAssistant(sess *Session, m *claude.AssistantMessage) {
	// The CLI streams each content block's deltas before delivering the
	// complete assistant message, so the counts observed so far belong to
	// THIS message. Take (and reset) them here: the fallback is per-message,
	// not per-turn — otherwise a later message whose deltas were lost would
	// never be emitted at all.
	textDeltas, thinkingDeltas := sess.takeDeltaCounts()

	for _, block := range m.Content {
		switch block.Type {
		case "text":
			// With partial streaming enabled, text is emitted from deltas.
			// Fall back to the complete message if no delta arrived.
			if block.Text != "" && textDeltas == 0 {
				a.notify(sess, acp.SessionUpdate{
					SessionUpdate: acp.UpdateAgentMessageChunk,
					Content:       acp.TextContent(block.Text),
				})
			}
		case "thinking":
			if block.Thinking != "" && thinkingDeltas == 0 {
				a.notify(sess, acp.SessionUpdate{
					SessionUpdate: acp.UpdateAgentThoughtChunk,
					Content:       acp.TextContent(block.Thinking),
				})
			}
		case "tool_use":
			title := toolTitle(block.Name, block.Input)
			kind := toolKind(block.Name)
			if sess.noteToolCall(block.ID) {
				a.notify(sess, acp.SessionUpdate{
					SessionUpdate: acp.UpdateToolCall,
					ToolCallID:    block.ID,
					Title:         title,
					Kind:          kind,
					Status:        acp.ToolStatusInProgress,
					RawInput:      block.Input,
				})
			} else {
				a.notify(sess, acp.SessionUpdate{
					SessionUpdate: acp.UpdateToolCallUpdate,
					ToolCallID:    block.ID,
					Title:         title,
					Kind:          kind,
					Status:        acp.ToolStatusInProgress,
				})
			}
		}
	}
}

func (a *Agent) handleUser(sess *Session, m *claude.UserMessage) {
	for _, block := range m.Content {
		if block.Type != "tool_result" {
			continue
		}
		status := acp.ToolStatusCompleted
		if block.IsError {
			status = acp.ToolStatusFailed
		}
		a.notify(sess, acp.SessionUpdate{
			SessionUpdate: acp.UpdateToolCallUpdate,
			ToolCallID:    block.ToolUseID,
			Status:        status,
			Content:       toolResultContent(block.Content),
		})
	}
}

func (a *Agent) resultStopReason(sess *Session, m *claude.ResultMessage) string {
	if sess.wasCancelled() {
		return acp.StopReasonCancelled
	}
	switch m.TerminalReason {
	case "aborted_tools", "aborted_streaming", "user_interrupt":
		return acp.StopReasonCancelled
	}
	switch m.Subtype {
	case "error_max_turns":
		return acp.StopReasonMaxTurnRequests
	}
	switch m.StopReason {
	case "max_tokens":
		return acp.StopReasonMaxTokens
	case "refusal":
		return acp.StopReasonRefusal
	}
	return acp.StopReasonEndTurn
}

func (a *Agent) resultError(m *claude.ResultMessage) *rpc.Error {
	if !m.IsError {
		return nil
	}
	message := strings.TrimSpace(m.Result)
	if message == "" && len(m.Errors) > 0 {
		message = fmt.Sprint(m.Errors[0])
	}
	if message == "" {
		message = "claude reported an error"
	}
	return rpc.ErrInternalMsg(message)
}

func (a *Agent) exitError(sess *Session) string {
	tail := strings.TrimSpace(sess.client.StderrTail())
	if tail == "" {
		return "claude process exited"
	}
	if len(tail) > 2000 {
		tail = tail[len(tail)-2000:]
	}
	return "claude process exited: " + tail
}

// drainTurn consumes messages until the cancelled turn's result arrives. It
// reports false when the turn did not settle in time.
func drainTurn(sess *Session) bool {
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		select {
		case msg, ok := <-sess.client.Events():
			if !ok {
				return true
			}
			if _, isResult := msg.(*claude.ResultMessage); isResult {
				return true
			}
		case <-timer.C:
			return false
		}
	}
}

func promptText(blocks []acp.ContentBlock) string {
	var b strings.Builder
	appendLine := func(s string) {
		if s == "" {
			return
		}
		if b.Len() > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(s)
	}
	for _, block := range blocks {
		switch block.Type {
		case "text":
			appendLine(block.Text)
		case "resource_link":
			// resource_link is a baseline ACP content type. Render it as a
			// readable reference; file:// URIs become local paths.
			ref := block.Name
			if ref == "" {
				ref = block.URI
			}
			uri := block.URI
			if strings.HasPrefix(uri, "file://") {
				if u, err := url.Parse(uri); err == nil && u.Path != "" {
					uri = u.Path
				}
			}
			if uri != "" && uri != ref {
				ref += " (" + uri + ")"
			}
			appendLine("Resource: " + ref)
		}
	}
	return b.String()
}

func toolResultContent(v any) any {
	switch content := v.(type) {
	case string:
		if content == "" {
			return nil
		}
		return []any{map[string]any{
			"type":    "content",
			"content": map[string]any{"type": "text", "text": content},
		}}
	case []any:
		out := make([]any, 0, len(content))
		for _, item := range content {
			m, ok := item.(map[string]any)
			if !ok || asString(m["type"]) != "text" {
				continue
			}
			text := asString(m["text"])
			if text == "" {
				continue
			}
			out = append(out, map[string]any{
				"type":    "content",
				"content": map[string]any{"type": "text", "text": text},
			})
		}
		if len(out) == 0 {
			return nil
		}
		return out
	}
	return nil
}

func asString(v any) string {
	s, _ := v.(string)
	return s
}
