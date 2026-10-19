// Command e2e drives a running claude-acp server through a basic ACP flow:
// initialize, session/new, session/list, a text prompt, and a prompt that
// triggers a permission request (auto-approved).
package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type message map[string]any

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "usage: e2e <ws-url> <cwd>\n")
		os.Exit(2)
	}
	url, cwd := os.Args[1], os.Args[2]
	mode := ""
	if len(os.Args) > 3 {
		mode = os.Args[3]
	}
	listOnly := mode == "list"
	loadMode := mode == "load"
	cancelMode := mode == "cancel"
	cancelRace := mode == "cancel-race"
	modesMode := mode == "modes"

	headers := http.Header{}
	if token := os.Getenv("CLAUDE_ACP_TOKEN"); token != "" {
		headers.Set("Authorization", "Bearer "+token)
	}
	conn, _, err := websocket.DefaultDialer.Dial(url, headers)
	if err != nil {
		fatal("dial: %v", err)
	}
	defer conn.Close()

	// Bridge preamble.
	preamble := read(conn)
	fmt.Printf("preamble: %v\n", preamble)

	nextID := 1
	call := func(method string, params any) message {
		id := nextID
		nextID++
		write(conn, message{"jsonrpc": "2.0", "id": id, "method": method, "params": params})
		for {
			msg := read(conn)
			if method_ := str(msg["method"]); method_ == "session/request_permission" {
				fmt.Printf("[permission] %v\n", msg["params"])
				write(conn, message{
					"jsonrpc": "2.0",
					"id":      msg["id"],
					"result":  message{"outcome": message{"outcome": "selected", "optionId": "allow"}},
				})
				continue
			}
			if fmt.Sprint(msg["id"]) == fmt.Sprint(id) {
				if e := msg["error"]; e != nil {
					fmt.Printf("%s -> error: %v\n", method, e)
					os.Exit(1)
				}
				return msg
			}
			if method_ := str(msg["method"]); method_ != "" {
				printUpdate(msg)
			}
		}
	}

	initResp := call("initialize", message{
		"protocolVersion":    1,
		"clientCapabilities": message{"fs": message{"readTextFile": false, "writeTextFile": false}},
		"clientInfo":         message{"name": "claude-acp-e2e", "version": "0"},
	})
	fmt.Printf("initialize -> %v\n", initResp["result"])

	if listOnly || loadMode {
		listResp := call("session/list", message{"cwd": cwd})
		result, _ := listResp["result"].(map[string]any)
		sessions, _ := result["sessions"].([]any)
		fmt.Printf("session/list -> %d sessions\n", len(sessions))
		for i, s := range sessions {
			if i >= 5 {
				break
			}
			fmt.Printf("  %v\n", s)
		}
		if listOnly {
			return
		}
		if len(sessions) == 0 {
			fatal("no sessions to load")
		}
		first, _ := sessions[0].(map[string]any)
		sid, _ := first["sessionId"].(string)
		loadResp := call("session/load", message{"sessionId": sid, "cwd": cwd, "mcpServers": []any{}})
		fmt.Printf("session/load -> %v\n", loadResp["result"])
		_ = call("session/close", message{"sessionId": sid})
		return
	}

	sessResp := call("session/new", message{"cwd": cwd, "mcpServers": []any{}})
	result, _ := sessResp["result"].(map[string]any)
	sid, _ := result["sessionId"].(string)
	if sid == "" {
		fatal("no sessionId in %v", sessResp)
	}
	fmt.Printf("session/new -> %s\n", sid)
	if modes, ok := result["modes"]; ok {
		b, _ := json.Marshal(modes)
		fmt.Printf("modes -> %s\n", b)
	}
	if opts, ok := result["configOptions"]; ok {
		b, _ := json.Marshal(opts)
		fmt.Printf("configOptions -> %s\n", b)
	}

	if modesMode {
		for _, id := range []string{"auto", "dontAsk", "plan", "acceptEdits", "manual", "bypassPermissions"} {
			resp := call("session/set_mode", message{"sessionId": sid, "modeId": id})
			fmt.Printf("set_mode %s -> %v\n", id, resp["result"])
		}
		resp := call("session/set_config_option", message{"sessionId": sid, "configId": "mode", "value": "dontAsk"})
		fmt.Printf("set_config_option mode=dontAsk -> %v\n", resp["result"])
		_ = call("session/close", message{"sessionId": sid})
		return
	}

	if cancelMode || cancelRace {
		id := nextID
		nextID++
		write(conn, message{
			"jsonrpc": "2.0", "id": id, "method": "session/prompt",
			"params": message{
				"sessionId": sid,
				"prompt":    []any{message{"type": "text", "text": "Run `sleep 30` and then report done."}},
			},
		})
		if cancelRace {
			write(conn, message{
				"jsonrpc": "2.0", "method": "session/cancel",
				"params": message{"sessionId": sid},
			})
		} else {
			go func() {
				time.Sleep(2 * time.Second)
				write(conn, message{
					"jsonrpc": "2.0", "method": "session/cancel",
					"params": message{"sessionId": sid},
				})
			}()
		}
		for {
			msg := read(conn)
			if str(msg["method"]) == "session/request_permission" {
				write(conn, message{
					"jsonrpc": "2.0", "id": msg["id"],
					"result": message{"outcome": message{"outcome": "selected", "optionId": "allow"}},
				})
				continue
			}
			if str(msg["method"]) != "" {
				printUpdate(msg)
				continue
			}
			if fmt.Sprint(msg["id"]) == fmt.Sprint(id) {
				result, _ := msg["result"].(map[string]any)
				stop := str(result["stopReason"])
				fmt.Printf("cancel -> %v\n", stop)
				if stop != "cancelled" {
					os.Exit(1)
				}
				_ = call("session/close", message{"sessionId": sid})
				return
			}
		}
	}

	cfgResp := call("session/set_config_option", message{"sessionId": sid, "configId": "model", "value": "default"})
	fmt.Printf("set model -> ok=%v\n", cfgResp["result"] != nil)
	cfgResp = call("session/set_config_option", message{"sessionId": sid, "configId": "effort", "value": "default"})
	fmt.Printf("set effort -> ok=%v\n", cfgResp["result"] != nil)

	listResp := call("session/list", message{"cwd": cwd})
	if r, ok := listResp["result"].(map[string]any); ok {
		if sessions, ok := r["sessions"].([]any); ok {
			fmt.Printf("session/list -> %d sessions\n", len(sessions))
		}
	}

	stop := call("session/prompt", message{
		"sessionId": sid,
		"prompt":    []any{message{"type": "text", "text": "Reply with exactly: HELLO ACP"}},
	})
	fmt.Printf("prompt 1 -> %v\n", stop["result"])

	stop = call("session/prompt", message{
		"sessionId": sid,
		"prompt":    []any{message{"type": "text", "text": "Run `ls` in the current directory and report the file names. Ask for permission if needed."}},
	})
	fmt.Printf("prompt 2 -> %v\n", stop["result"])

	stop = call("session/prompt", message{
		"sessionId": sid,
		"prompt":    []any{message{"type": "text", "text": "Create a file named hello.txt in the current directory containing the single line: hi. Use the Write tool."}},
	})
	fmt.Printf("prompt 3 -> %v\n", stop["result"])

	_ = call("session/close", message{"sessionId": sid})
	fmt.Println("done")
}

func printUpdate(msg message) {
	params, _ := msg["params"].(map[string]any)
	update, _ := params["update"].(map[string]any)
	if update == nil {
		return
	}
	switch str(update["sessionUpdate"]) {
	case "agent_message_chunk":
		if c, ok := update["content"].(map[string]any); ok {
			fmt.Print(str(c["text"]))
		}
	case "agent_thought_chunk":
	case "tool_call":
		fmt.Printf("\n[tool_call %s %s %s]\n", str(update["toolCallId"]), str(update["kind"]), str(update["title"]))
	case "tool_call_update":
		fmt.Printf("[tool_update %s %s]\n", str(update["toolCallId"]), str(update["status"]))
	}
}

func read(conn *websocket.Conn) message {
	_ = conn.SetReadDeadline(time.Now().Add(180 * time.Second))
	_, data, err := conn.ReadMessage()
	if err != nil {
		fatal("read: %v", err)
	}
	var m message
	if err := json.Unmarshal(data, &m); err != nil {
		fatal("decode: %v (%s)", err, data)
	}
	return m
}

var writeMu sync.Mutex

func write(conn *websocket.Conn, m message) {
	b, err := json.Marshal(m)
	if err != nil {
		fatal("encode: %v", err)
	}
	// Serialize writes: the cancel test writes from a goroutine while the
	// main loop may answer a permission request.
	writeMu.Lock()
	defer writeMu.Unlock()
	if err := conn.WriteMessage(websocket.TextMessage, b); err != nil {
		fatal("write: %v", err)
	}
}

func str(v any) string {
	s, _ := v.(string)
	return s
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
