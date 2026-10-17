package agent

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/coxlong/claude-acp/internal/acp"
	"github.com/coxlong/claude-acp/internal/claude"
	"github.com/coxlong/claude-acp/internal/rpc"
)

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

	sess.promptMu.Lock()
	defer sess.promptMu.Unlock()

	sess.resetTurn()
	sess.setTurnCtx(ctx)
	defer sess.resetTurn()

	if err := sess.client.SendUserText(text); err != nil {
		return acp.PromptResponse{}, rpc.ErrInternalMsg(err.Error())
	}

	for {
		select {
		case msg, ok := <-sess.client.Events():
			if !ok {
				return acp.PromptResponse{}, rpc.ErrInternalMsg(a.exitError(sess))
			}
			done, stop, rerr := a.handleEvent(sess, msg)
			if rerr != nil {
				return acp.PromptResponse{}, rerr
			}
			if done {
				return acp.PromptResponse{StopReason: stop}, nil
			}
		case <-ctx.Done():
			// The turn was cancelled at the protocol level. Ask the CLI to
			// stop, then drain the rest of the turn so the next prompt does
			// not consume its messages.
			_ = sess.client.Interrupt(context.Background())
			drainTurn(sess)
			return acp.PromptResponse{}, rpc.ErrCancelled
		}
	}
}

func (a *Agent) handleEvent(sess *Session, msg claude.Message) (done bool, stop string, rerr *rpc.Error) {
	switch m := msg.(type) {
	case *claude.StreamEvent:
		a.handleStreamEvent(sess, m)
	case *claude.AssistantMessage:
		a.handleAssistant(sess, m)
	case *claude.UserMessage:
		a.handleUser(sess, m)
	case *claude.ResultMessage:
		return true, a.resultStopReason(sess, m), a.resultError(m)
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
	for _, block := range m.Content {
		switch block.Type {
		case "text":
			// With partial streaming enabled, text is emitted from deltas.
			// Fall back to the complete message if no delta was seen.
			if block.Text != "" && !sess.sawTextDelta() {
				a.notify(sess, acp.SessionUpdate{
					SessionUpdate: acp.UpdateAgentMessageChunk,
					Content:       acp.TextContent(block.Text),
				})
			}
		case "thinking":
			if block.Thinking != "" && !sess.sawThinkingDelta() {
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

func drainTurn(sess *Session) {
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		select {
		case msg, ok := <-sess.client.Events():
			if !ok {
				return
			}
			if _, isResult := msg.(*claude.ResultMessage); isResult {
				return
			}
		case <-timer.C:
			return
		}
	}
}

func promptText(blocks []acp.ContentBlock) string {
	var b strings.Builder
	for _, block := range blocks {
		if block.Type == "text" && block.Text != "" {
			if b.Len() > 0 {
				b.WriteByte('\n')
			}
			b.WriteString(block.Text)
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
