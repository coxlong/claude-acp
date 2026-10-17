package agent

import (
	"github.com/coxlong/claude-acp/internal/acp"
	"github.com/coxlong/claude-acp/internal/claude"
)

// requestPermission bridges a CLI can_use_tool control request to an ACP
// session/request_permission call.
func (a *Agent) requestPermission(sess *Session, req *claude.PermissionRequest) (claude.PermissionResult, error) {
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

	options := []acp.PermissionOption{
		{OptionID: "allow", Name: "Allow Once", Kind: acp.PermissionAllowOnce},
		{OptionID: "allow_always", Name: "Always Allow", Kind: acp.PermissionAllowAlways},
		{OptionID: "reject", Name: "Deny", Kind: acp.PermissionRejectOnce},
		{OptionID: "reject_always", Name: "Always Deny", Kind: acp.PermissionRejectAlways},
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
	case "allow", "allow_always":
		return claude.Allow(req.Input), nil
	default:
		return claude.Deny("User denied this tool call"), nil
	}
}
