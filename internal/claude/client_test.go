package claude

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"
)

// TestMain lets the test binary act as a fake Claude CLI when invoked with
// FAKE_CLI=1. This gives deterministic coverage of the stream-json and
// control protocols without an account or network.
func TestMain(m *testing.M) {
	if os.Getenv("FAKE_CLI") == "1" {
		fakeCLI()
		return
	}
	os.Exit(m.Run())
}

func fakeCLI() {
	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 1<<20), 16<<20)
	out := bufio.NewWriter(os.Stdout)

	write := func(v any) {
		b, _ := json.Marshal(v)
		_, _ = out.Write(b)
		_ = out.WriteByte('\n')
		_ = out.Flush()
	}

	pendingPermission := ""

	for in.Scan() {
		var env struct {
			Type      string `json:"type"`
			RequestID string `json:"request_id"`
			Request   struct {
				Subtype string `json:"subtype"`
			} `json:"request"`
			Response struct {
				RequestID string `json:"request_id"`
			} `json:"response"`
			Message struct {
				Content []struct {
					Type string `json:"type"`
					Text string `json:"text"`
				} `json:"content"`
			} `json:"message"`
		}
		if err := json.Unmarshal(in.Bytes(), &env); err != nil {
			continue
		}

		switch env.Type {
		case "control_request":
			result := map[string]any{}
			if env.Request.Subtype == "initialize" {
				result["models"] = []map[string]any{{
					"value":                 "default",
					"displayName":           "Default",
					"supportsEffort":        true,
					"supportedEffortLevels": []string{"low", "high"},
				}}
			}
			write(map[string]any{
				"type": "control_response",
				"response": map[string]any{
					"subtype":    "success",
					"request_id": env.RequestID,
					"response":   result,
				},
			})

		case "control_response":
			if env.Response.RequestID == pendingPermission {
				pendingPermission = ""
				write(map[string]any{
					"type": "assistant",
					"message": map[string]any{
						"content": []map[string]any{{
							"type":  "tool_use",
							"id":    "tool-1",
							"name":  "Write",
							"input": map[string]any{"file_path": "/tmp/x"},
						}},
					},
				})
				write(map[string]any{
					"type": "user",
					"message": map[string]any{
						"content": []map[string]any{{
							"type":        "tool_result",
							"tool_use_id": "tool-1",
							"content":     "ok",
						}},
					},
				})
				write(map[string]any{"type": "result", "subtype": "success", "is_error": false, "result": "done"})
			}

		case "user":
			var text strings.Builder
			for _, block := range env.Message.Content {
				if block.Type == "text" {
					text.WriteString(block.Text)
				}
			}
			if strings.Contains(text.String(), "use tool") {
				pendingPermission = "perm-1"
				write(map[string]any{
					"type":       "control_request",
					"request_id": "perm-1",
					"request": map[string]any{
						"subtype":     "can_use_tool",
						"tool_name":   "Write",
						"tool_use_id": "tool-1",
						"input":       map[string]any{"file_path": "/tmp/x"},
					},
				})
				continue
			}
			write(map[string]any{
				"type": "stream_event",
				"event": map[string]any{
					"type":  "content_block_delta",
					"delta": map[string]any{"type": "text_delta", "text": "hello "},
				},
			})
			write(map[string]any{
				"type":    "assistant",
				"message": map[string]any{"content": []map[string]any{{"type": "text", "text": "hello "}}},
			})
			write(map[string]any{"type": "result", "subtype": "success", "is_error": false, "result": "hello "})
		}
	}
}

func startFake(t *testing.T, canUse func(*PermissionRequest) (PermissionResult, error)) *Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	c, err := Start(ctx, Options{
		CliPath:    os.Args[0],
		SessionID:  "test-session",
		Env:        map[string]string{"FAKE_CLI": "1"},
		CanUseTool: canUse,
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func waitResult(t *testing.T, c *Client) *ResultMessage {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case msg, ok := <-c.Events():
			if !ok {
				t.Fatal("events closed before result")
			}
			if r, ok := msg.(*ResultMessage); ok {
				return r
			}
		case <-deadline:
			t.Fatal("timed out waiting for result")
		}
	}
}

func TestClientStreaming(t *testing.T) {
	c := startFake(t, nil)

	if models := c.Models(); len(models) != 1 || models[0].Name() != "default" {
		t.Fatalf("models = %#v", models)
	}
	if levels := c.Models()[0].SupportedEffortLevels; len(levels) != 2 {
		t.Fatalf("model effort levels = %#v", levels)
	}

	if err := c.SendUserText("hello"); err != nil {
		t.Fatalf("send: %v", err)
	}

	sawDelta := false
	deadline := time.After(5 * time.Second)
	for {
		select {
		case msg, ok := <-c.Events():
			if !ok {
				t.Fatal("events closed")
			}
			switch m := msg.(type) {
			case *StreamEvent:
				delta, _ := m.Event["delta"].(map[string]any)
				if asString(delta["type"]) == "text_delta" && asString(delta["text"]) == "hello " {
					sawDelta = true
				}
			case *ResultMessage:
				if !sawDelta {
					t.Fatal("no text delta observed")
				}
				if m.IsError {
					t.Fatalf("result is error: %#v", m)
				}
				return
			}
		case <-deadline:
			t.Fatal("timed out")
		}
	}
}

func TestClientPermission(t *testing.T) {
	called := false
	c := startFake(t, func(req *PermissionRequest) (PermissionResult, error) {
		called = true
		if req.ToolName != "Write" || req.ToolUseID != "tool-1" {
			t.Fatalf("permission request = %#v", req)
		}
		return Allow(req.Input), nil
	})

	if err := c.SendUserText("please use tool"); err != nil {
		t.Fatalf("send: %v", err)
	}
	r := waitResult(t, c)
	if r.IsError {
		t.Fatalf("result is error: %#v", r)
	}
	if !called {
		t.Fatal("CanUseTool was not invoked")
	}
}

func TestControlRequests(t *testing.T) {
	c := startFake(t, nil)
	ctx := context.Background()

	if err := c.SetModel(ctx, "default"); err != nil {
		t.Fatalf("set model: %v", err)
	}
	if err := c.SetPermissionMode(ctx, "plan"); err != nil {
		t.Fatalf("set mode: %v", err)
	}
	if err := c.SetEffort(ctx, "high"); err != nil {
		t.Fatalf("set effort: %v", err)
	}
	if err := c.Interrupt(ctx); err != nil {
		t.Fatalf("interrupt: %v", err)
	}
}
