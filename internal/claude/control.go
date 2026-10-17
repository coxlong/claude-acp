package claude

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type controlResult struct {
	response json.RawMessage
	err      error
}

func newControlID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "control"
	}
	return hex.EncodeToString(b[:])
}

// request sends a control request and waits for its response.
func (c *Client) request(ctx context.Context, subtype string, payload map[string]any, out any) error {
	// Every control request needs a deadline: a wedged CLI must not block a
	// session forever.
	timeout := 10 * time.Second
	switch subtype {
	case "initialize":
		timeout = 60 * time.Second
	case "interrupt":
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	id := newControlID()
	req := make(map[string]any, len(payload)+1)
	req["subtype"] = subtype
	for k, v := range payload {
		req[k] = v
	}

	ch := make(chan controlResult, 1)
	c.pendingMu.Lock()
	c.pending[id] = ch
	c.pendingMu.Unlock()
	defer func() {
		c.pendingMu.Lock()
		delete(c.pending, id)
		c.pendingMu.Unlock()
	}()

	envelope := map[string]any{
		"type":       "control_request",
		"request_id": id,
		"request":    req,
	}
	if err := c.writeJSON(envelope); err != nil {
		return err
	}

	select {
	case res := <-ch:
		if res.err != nil {
			return res.err
		}
		if out != nil && len(res.response) > 0 {
			return json.Unmarshal(res.response, out)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return errors.New("claude process exited")
	}
}

func (c *Client) deliverControl(env controlResponse) {
	c.pendingMu.Lock()
	ch := c.pending[env.Response.RequestID]
	c.pendingMu.Unlock()
	if ch == nil {
		c.log.Debug("claude: response for unknown control request",
			"requestId", env.Response.RequestID,
			"subtype", env.Response.Subtype,
			"response", string(env.Response.Response),
			"error", env.Response.Error)
		return
	}
	if env.Response.Subtype == "error" {
		msg := env.Response.Error
		if msg == "" {
			msg = "control request failed"
		}
		ch <- controlResult{err: fmt.Errorf("%s", msg)}
		return
	}
	ch <- controlResult{response: env.Response.Response}
}

func (c *Client) respondControl(id string, result any) {
	_ = c.writeJSON(map[string]any{
		"type": "control_response",
		"response": map[string]any{
			"subtype":    "success",
			"request_id": id,
			"response":   result,
		},
	})
}

func (c *Client) respondControlError(id, message string) {
	_ = c.writeJSON(map[string]any{
		"type": "control_response",
		"response": map[string]any{
			"subtype":    "error",
			"request_id": id,
			"error":      message,
		},
	})
}

func (c *Client) handleControlRequest(env controlRequest) {
	switch env.Request.Subtype {
	case "can_use_tool":
		req := &PermissionRequest{
			ToolName:       env.Request.ToolName,
			Input:          env.Request.Input,
			ToolUseID:      env.Request.ToolUseID,
			Suggestions:    env.Request.PermissionSuggestions,
			DecisionReason: env.Request.DecisionReason,
			BlockedPath:    env.Request.BlockedPath,
			Title:          env.Request.Title,
			DisplayName:    env.Request.DisplayName,
			Description:    env.Request.Description,
			AgentID:        env.Request.AgentID,
			RequestID:      env.RequestID,
		}

		result := Deny("permission handler unavailable")
		if c.opts.CanUseTool != nil {
			got, err := c.opts.CanUseTool(req)
			if err != nil {
				result = Deny(err.Error())
			} else {
				result = got
			}
		}
		if result.Behavior != "allow" && result.Behavior != "deny" {
			result = Deny("invalid permission decision")
		}

		payload := map[string]any{
			"behavior":  result.Behavior,
			"toolUseID": env.Request.ToolUseID,
		}
		if result.Behavior == "allow" {
			if result.UpdatedInput != nil {
				payload["updatedInput"] = result.UpdatedInput
			}
		} else {
			if result.Message != "" {
				payload["message"] = result.Message
			}
			if result.Interrupt {
				payload["interrupt"] = true
			}
		}
		c.respondControl(env.RequestID, payload)

	case "elicitation":
		// The agent does not advertise elicitation support; decline cleanly.
		c.respondControl(env.RequestID, map[string]any{"action": "decline"})

	default:
		c.respondControlError(env.RequestID, "unsupported control request: "+env.Request.Subtype)
	}
}

func (c *Client) initialize(ctx context.Context) error {
	var result map[string]any
	err := c.request(ctx, "initialize", map[string]any{
		"systemPrompt": map[string]any{"type": "preset", "preset": "claude_code"},
	}, &result)
	if err != nil {
		return err
	}
	c.initMu.Lock()
	c.initResult = result
	c.initMu.Unlock()
	return nil
}

// InitResult returns the raw initialization result (models, commands, ...).
func (c *Client) InitResult() map[string]any {
	c.initMu.Lock()
	defer c.initMu.Unlock()
	return c.initResult
}

// Models returns the models advertised by the CLI.
func (c *Client) Models() []ModelInfo {
	raw := c.InitResult()
	if raw == nil {
		return nil
	}
	b, err := json.Marshal(raw["models"])
	if err != nil {
		return nil
	}
	var models []ModelInfo
	if err := json.Unmarshal(b, &models); err != nil {
		return nil
	}
	return models
}

// SupportedEffortLevels returns the effort levels reported by the CLI.
func (c *Client) SupportedEffortLevels() []string {
	raw := c.InitResult()
	if raw == nil {
		return nil
	}
	return StringSlice(raw["supportedEffortLevels"])
}

// StringSlice coerces a JSON value into []string.
func StringSlice(v any) []string {
	items, ok := v.([]any)
	if !ok {
		if ss, ok := v.([]string); ok {
			return ss
		}
		return nil
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		if s, ok := item.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// Interrupt cancels the current turn.
func (c *Client) Interrupt(ctx context.Context) error {
	return c.request(ctx, "interrupt", nil, nil)
}

// SetModel switches the model at runtime.
func (c *Client) SetModel(ctx context.Context, model string) error {
	return c.request(ctx, "set_model", map[string]any{"model": model}, nil)
}

// SetPermissionMode switches the permission mode at runtime.
func (c *Client) SetPermissionMode(ctx context.Context, mode string) error {
	return c.request(ctx, "set_permission_mode", map[string]any{"mode": mode}, nil)
}

// SetEffort switches the effort level at runtime.
func (c *Client) SetEffort(ctx context.Context, level string) error {
	return c.request(ctx, "apply_flag_settings", map[string]any{
		"settings": map[string]any{"effortLevel": level},
	}, nil)
}
