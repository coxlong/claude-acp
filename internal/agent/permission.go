package agent

import (
	"errors"

	"github.com/coxlong/claude-acp/internal/acp"
	"github.com/coxlong/claude-acp/internal/claude"
	"github.com/coxlong/claude-acp/internal/rpc"
)

// requestPermission bridges a CLI can_use_tool control request to an ACP
// session/request_permission call.
func (a *Agent) requestPermission(sess *Session, req *claude.PermissionRequest) (claude.PermissionResult, error) {
	if !sess.isRunning() {
		// The turn already ended (e.g. an interrupted tool); do not block on
		// a client that will never answer.
		return claude.Deny("turn already ended"), nil
	}

	title := req.Title
	if title == "" {
		title = req.DisplayName
	}
	if title == "" {
		title = toolTitle(req.ToolName, req.Input)
	}
	kind := toolKind(req.ToolName)

	if sess.noteToolCall(req.ToolUseID) {
		a.notify(sess, acp.SessionUpdate{
			SessionUpdate: acp.UpdateToolCall,
			ToolCallID:    req.ToolUseID,
			Title:         title,
			Kind:          kind,
			Status:        acp.ToolStatusPending,
			RawInput:      req.Input,
		})
	} else {
		a.notify(sess, acp.SessionUpdate{
			SessionUpdate: acp.UpdateToolCallUpdate,
			ToolCallID:    req.ToolUseID,
			Title:         title,
			Kind:          kind,
			Status:        acp.ToolStatusPending,
		})
	}

	// Persistent deny is not supported by the CLI permission result, so only
	// a one-shot deny is offered.
	options := []acp.PermissionOption{
		{OptionID: "allow", Name: "Allow Once", Kind: acp.PermissionAllowOnce},
		{OptionID: "allow_always", Name: "Always Allow", Kind: acp.PermissionAllowAlways},
		{OptionID: "reject", Name: "Deny", Kind: acp.PermissionRejectOnce},
	}

	call := acp.RequestPermissionRequest{
		SessionID: sess.id,
		ToolCall: acp.ToolCall{
			ToolCallID: req.ToolUseID,
			Title:      title,
			Kind:       kind,
			Status:     acp.ToolStatusPending,
			RawInput:   req.Input,
		},
		Options: options,
	}

	var resp acp.RequestPermissionResponse
	if err := a.conn.Call(sess.turnContext(), acp.ClientRequestPermission, call, &resp); err != nil {
		var rerr *rpc.Error
		if errors.As(err, &rerr) && rerr.Code == -32800 {
			// The turn was cancelled while the dialog was open. Deny with
			// interrupt so the CLI stops instead of issuing the next tool
			// call before the separate interrupt control request lands.
			return claude.PermissionResult{
				Behavior:  "deny",
				Message:   "Tool call cancelled",
				Interrupt: true,
			}, nil
		}
		return claude.Deny("permission request failed: " + err.Error()), nil
	}

	if resp.Outcome.Outcome == "cancelled" {
		return claude.PermissionResult{
			Behavior:  "deny",
			Message:   "Tool call cancelled",
			Interrupt: true,
		}, nil
	}
	switch resp.Outcome.OptionID {
	case "allow":
		return claude.Allow(req.Input), nil
	case "allow_always":
		updates := req.Suggestions
		if len(updates) == 0 {
			updates = []map[string]any{{
				"type":        "addRules",
				"rules":       []map[string]any{{"toolName": req.ToolName}},
				"behavior":    "allow",
				"destination": "session",
			}}
		}
		return claude.PermissionResult{
			Behavior:           "allow",
			UpdatedInput:       req.Input,
			UpdatedPermissions: updates,
		}, nil
	default:
		return claude.Deny("User denied this tool call"), nil
	}
}
