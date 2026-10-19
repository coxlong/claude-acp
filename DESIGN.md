# claude-acp 设计方案

> 状态：设计定稿（2026-10-18）
> 目标：用 Go 实现一个**通用的 ACP（Agent Client Protocol）agent**，直接驱动 Claude Code CLI，通过 **WebSocket** 对外提供服务。
> 第一期（P1）只实现 omux 客户端实际使用的协议面，其余方法返回 `-32601` 且不声明能力。

---

## 1. 定位与范围

### 1.1 是什么

- 服务端组件：手机/远程 ACP 客户端通过 `ws://` / `wss://` 连接。
- 协议：ACP `protocolVersion: 1`，NDJSON 帧，JSON-RPC 2.0。
- Agent 后端：本机 `claude` CLI 子进程（Claude Code）。
- 通用性：不绑定 omux；任何能说 ACP-over-WebSocket 的客户端都可用。

### 1.2 明确不做（P1 非目标）

| 非目标 | 原因 |
|---|---|
| stdio 传输 | 已明确不需要 |
| `fs/read_text_file`、`fs/write_text_file`、`terminal/*` | 这些是 client 侧方法；本 agent 不调用，也不实现 |
| elicitation、providers、goal、steer、session-failure 扩展 | P1 用不到，后续按需 |
| 多租户 / 多 Claude 账号 | P1 单用户，认证由服务器上的 CLI 本地凭据完成 |
| `session/fork`、`session/resume`、`session/delete` | P2；P1 不声明能力 |
| 图片 / embeddedContext / subagent 转写 | P2 |

---

## 2. 关键决策（ADR）

### D1：WebSocket 是唯一传输

- 服务器默认监听 `127.0.0.1:3000`。
- 绑定非 loopback 地址时**必须**配置 token，否则启动失败。
- 传输层只负责字节与连接生命周期，不理解 ACP 语义。

### D2：不自研 ACP 协议之外，也不依赖非官方 ACP SDK

- 官方 ACP SDK 只有 TypeScript / Rust / Kotlin / Python / Java，**没有 Go**。
- `coder/acp-go-sdk` 是社区库（官方文档 Community 列表），不作为依赖。
- ACP 方法与类型由本项目实现，**以官方 JSON Schema 为唯一事实来源**：
  - vendor 官方 `schema.json` 到 `schema/`，锁定版本；
  - CI 用 schema 校验收发的协议样本。

### D3：JSON-RPC 层自研

- Go 标准库 `net/rpc/jsonrpc` 只支持 JSON-RPC 1.0，无通知/双向调用，不可用。
- 生态库（`sourcegraph/jsonrpc2` 257 stars、`creachadair/jrpc2` 93 stars）均非主流基础设施；且 ACP 需要的 `$/cancel_request`、跨重连、去重都要绕开库的抽象。
- JSON-RPC 2.0 规范很小，自研约 250–350 行，完全可控。

### D4：运行时第三方依赖只有 WebSocket 库

| 依赖 | 版本 | 用途 |
|---|---|---|
| `github.com/gorilla/websocket` | v1.5.3 | WebSocket 协议、帧、ping/pong |

其余全部使用标准库：`net/http`、`encoding/json`、`bufio`、`os/exec`、`context`、`log/slog`、`crypto/rand`、`sync`。

### D5：一个 ACP 会话 = 一个 claude 子进程

- 与 Claude Agent SDK 的 `query()` 模型一致：进程长驻，多轮复用。
- `session/close`、宽限期到期、进程异常退出时回收。

---

## 3. 仓库与版本

| 项 | 值 |
|---|---|
| 仓库名 | `claude-acp` |
| Go module | `github.com/coxlong/claude-acp`（按实际 owner 调整） |
| 二进制 | `claude-acp` |
| Go 版本 | **1.26**（本机 1.26.2；仅使用 1.21+ 特性，必要时可降级） |
| License | Apache-2.0 |
| 版本策略 | `v0.x`，跟随 ACP `protocolVersion: 1` |

### 启动方式

```bash
# 默认：WS 服务
claude-acp serve --addr 127.0.0.1:3000

# 对外服务
claude-acp serve --addr 0.0.0.0:3000 --token "$CLAUDE_ACP_TOKEN"

# 版本与自检
claude-acp version
claude-acp doctor        # 检查 claude CLI、凭据、配置目录
```

---

## 4. 架构

```
omux / 任意 ACP-over-WS 客户端
        │
        │  WebSocket (NDJSON, Bearer, acp.v1)
        ▼
┌────────────────────────── claude-acp ──────────────────────────┐
│                                                                │
│  internal/ws                                                   │
│  ┌──────────────┐   ┌──────────────────────────────────────┐   │
│  │ HTTP upgrade │──►│ Slot: clientId / conn / outbox        │   │
│  │ Bearer / TLS │   │  重连复用 / 回放 / 去重 / 宽限期       │   │
│  └──────────────┘   └───────────────┬──────────────────────┘   │
│                                     │ ObjectStream              │
│  internal/rpc                       ▼                          │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ JSON-RPC 2.0：信封 / id 关联 / 通知 / 取消（自研）          │  │
│  └────────────────────────┬─────────────────────────────────┘  │
│                           ▼                                    │
│  internal/acp                                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ ACP 方法与类型（自研）：initialize / session.* / 能力声明   │  │
│  └────────────────────────┬─────────────────────────────────┘  │
│                           ▼                                    │
│  internal/agent                                                │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ 语义翻译：CLI 消息 → session/update；权限桥接；configOptions│  │
│  └────────────────────────┬─────────────────────────────────┘  │
│                           ▼                                    │
│  internal/claude                                               │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ 子进程 / stream-json / control 协议（自研）                 │  │
│  └────────────────────────┬─────────────────────────────────┘  │
└───────────────────────────┼────────────────────────────────────┘
                            │ stdio (NDJSON)
                            ▼
                      claude CLI
```

### 4.1 两个关键边界

1. **物理连接 vs 逻辑连接**
   - 物理连接：一个 WebSocket 连接，断线即失效。
   - 逻辑连接：`Slot` + `rpc.Conn`，跨物理重连存活，持有会话、进行中的 prompt、待回的权限请求。
2. **协议层 vs 语义层**
   - `rpc` 只懂 JSON-RPC 2.0。
   - `acp` 只懂 ACP 方法。
   - `agent` 负责 ACP ↔ Claude 的语义翻译。
   - `claude` 只负责 CLI 子进程与 wire protocol。

---

## 5. 包结构

```
claude-acp/
├── cmd/claude-acp/
│   └── main.go                 # 子命令、flag、信号处理
├── internal/
│   ├── rpc/
│   │   ├── message.go          # Request / Response / Error 信封
│   │   ├── conn.go             # Call / Notify / id 关联 / 超时
│   │   ├── cancel.go           # $/cancel_request 与请求上下文
│   │   └── stream.go           # ObjectStream 接口
│   ├── acp/
│   │   ├── constants.go        # 方法名、错误码、协议版本
│   │   ├── types.go            # P1 所需的 ACP 类型
│   │   ├── server.go           # 方法分发（rpc.Handler 实现）
│   │   ├── capabilities.go     # initialize 能力协商
│   │   └── errors.go           # 错误映射
│   ├── claude/
│   │   ├── process.go          # spawn / 进程组 / 回收
│   │   ├── args.go             # 启动参数拼装
│   │   ├── stream.go           # stream-json 编解码
│   │   ├── control.go          # control_request/response 关联
│   │   ├── initialize.go       # initialize 握手（models/commands）
│   │   └── types.go            # CLI 消息类型
│   ├── agent/
│   │   ├── agent.go            # 会话管理
│   │   ├── translate.go        # CLI 消息 → ACP session/update
│   │   ├── permission.go       # can_use_tool → session/request_permission
│   │   ├── tools.go            # 工具名 → kind / title
│   │   ├── config_options.go   # model / effort / mode
│   │   └── history.go          # jsonl 解析、session/list、load 回放
│   ├── ws/
│   │   ├── server.go           # upgrade / Bearer / TLS / 监听策略
│   │   ├── slot.go             # clientId、outbox、会话、宽限期
│   │   ├── pump.go             # 物理连接的读/写 goroutine
│   │   └── dedupe.go           # 重复 requestId 响应缓存
│   └── config/
│       └── config.go
├── schema/
│   ├── schema.json             # vendor 官方 ACP schema
│   └── VERSION
├── testdata/
│   ├── cli/                    # 黄金 transcript
│   └── acp/                    # ACP 样本
├── Dockerfile
├── Makefile
└── README.md
```

---

## 6. WebSocket 传输设计

### 6.1 连接与槽位

```
Slot {
    clientID   string
    conn       atomic.Pointer[physicalConn]   // 断线时为 nil
    outbox     *outbox                        // agent → client 未送达帧
    inbound    chan []byte                    // client → agent 帧
    sessions   map[string]*session
    lastSeen   time.Time
    dedupe     *dedupeCache
}
```

- `Slot` 实现 `rpc.ObjectStream`：
  - `WriteObject` → 序列化为一行 JSON → 写入 `outbox`（从不阻塞 rpc 层）。
  - `ReadObject` → 从 `inbound` 阻塞读取。
- `rpc.Conn` 绑定 `Slot`，**不绑定物理连接**。

### 6.2 clientId 与前导帧

| 场景 | 行为 |
|---|---|
| 首次连接 | 生成 UUID；发送 `{"type":"connected","clientId":"<id>"}` |
| 带 `X-Client-Id` 且命中存活 Slot | 复用 Slot；发送 `{"type":"reconnect","clientId":"<id>"}`，随后按序回放 outbox |
| 带未知 `X-Client-Id` | 视为新连接，生成新 id，返回 `connected` |
| 不带 | 每次生成新 id |

前导帧不是 ACP 消息，属于桥接扩展；omux 已在客户端解析。

### 6.3 断线与重连

- 物理连接断开：只停止读/写 pump，**不关闭** `rpc.Conn`，会话继续运行。
- 宽限期 `--session-grace`（默认 10 分钟）：
  - 期间 agent 输出继续写入 outbox；
  - 到期后取消进行中的 turn、关闭 claude 进程、删除 Slot。
- 重连成功：先前导帧，再按入队顺序回放 outbox。
- 客户端重连后会 flush 自己的发件箱，因此可能重复发送请求——见 6.5。

### 6.4 心跳与保活

- 客户端发送的 `$/ping` 通知在 inbound pump 中直接吞掉，不进入 ACP 层。
- 服务端使用 gorilla 的 ping/pong 控制帧（每 30s，读超时 90s）。
- 不向客户端发 ACP 心跳。

### 6.5 请求去重

- rpc 层发出响应时，记录 `(clientID, requestID) → 响应帧`（最近 256 条）。
- inbound pump 收到请求时：
  - 若命中缓存：不执行，直接把缓存响应放入 outbox；
  - 若请求仍在执行：丢弃重复请求；原响应完成后只发一次。
- 通知（无 id）不去重。

### 6.6 背压与上限

| 项 | 默认值 | 超限行为 |
|---|---|---|
| outbox | 8 MiB | 取消活跃 turn、关闭该 Slot 的会话、清空 outbox |
| 每 Slot 会话数 | 16 | `session/new` 返回错误 |
| 单帧大小 | 32 MiB | 关闭连接并记录日志 |
| 空闲 Slot（无连接） | 10 分钟 | 回收 |

### 6.7 鉴权与安全

- `Authorization: Bearer <token>`，constant-time 比较。
- 未配置 token 时只允许 bind `127.0.0.1` / `::1`。
- 支持 `--tls-cert` / `--tls-key`；推荐反向代理终止 TLS。
- `--cwd-root`：可选的会话工作目录白名单。
- 不记录 prompt / 工具参数内容（`debug` 级别除外）。
- 子进程使用独立进程组（Unix `Setpgid`），退出时整组 kill。

### 6.8 子协议

- 接受 `acp.v1`，但不强制；未提供的客户端也可以连接。

---

## 7. ACP 层设计

### 7.1 协议版本与能力

```json
{
  "protocolVersion": 1,
  "agentCapabilities": {
    "loadSession": true,
    "promptCapabilities": {
      "image": false,
      "audio": false,
      "embeddedContext": false
    },
    "mcpCapabilities": { "http": true, "sse": true },
    "sessionCapabilities": {
      "list": {},
      "close": {}
    }
  },
  "authMethods": []
}
```

- `authMethods` 为空：认证由 claude CLI 的本地凭据完成，ACP 层没有账号体系。
- P1 不声明：`resume`、`fork`、`delete`、`promptCapabilities.image`。

### 7.2 P1 方法

| 方向 | 方法 | 说明 |
|---|---|---|
| client → agent | `initialize` | 能力协商 |
| client → agent | `authenticate` | no-op |
| client → agent | `session/new` | 创建会话并 spawn CLI |
| client → agent | `session/prompt` | 一轮对话 |
| client → agent | `session/cancel` | 中断当前 turn |
| client → agent | `session/load` | 恢复会话并回放历史 |
| client → agent | `session/list` | 列出历史会话 |
| client → agent | `session/close` | 关闭会话并回收进程 |
| client → agent | `session/set_mode` | 切换权限模式 |
| client → agent | `session/set_config_option` | 切换 model / effort |
| agent → client | `session/update` | 流式更新 |
| agent → client | `session/request_permission` | 工具审批 |
| 双方 | `$/cancel_request` | 取消进行中的请求 |

未实现的方法统一返回 JSON-RPC `-32601`。

### 7.3 核心类型（P1）

```go
type InitializeRequest struct {
    ProtocolVersion    int                `json:"protocolVersion"`
    ClientCapabilities ClientCapabilities `json:"clientCapabilities"`
    ClientInfo         *ClientInfo        `json:"clientInfo,omitempty"`
}

type NewSessionRequest struct {
    Cwd        string      `json:"cwd"`
    McpServers []McpServer `json:"mcpServers"`
    Meta       map[string]any `json:"_meta,omitempty"`
}

type NewSessionResponse struct {
    SessionID     string              `json:"sessionId"`
    Modes         *SessionModeState   `json:"modes,omitempty"`
    ConfigOptions []SessionConfigOption `json:"configOptions,omitempty"`
}

type PromptRequest struct {
    SessionID string         `json:"sessionId"`
    Prompt    []ContentBlock `json:"prompt"`
}

type PromptResponse struct {
    StopReason string `json:"stopReason"` // end_turn|max_tokens|max_turn_requests|refusal|cancelled
}

type SessionNotification struct {
    SessionID string        `json:"sessionId"`
    Update    SessionUpdate `json:"update"`
}
```

`SessionUpdate` 使用 `sessionUpdate` 字段判别：

| `sessionUpdate` | 用途 |
|---|---|
| `agent_message_chunk` | 助手文本增量 |
| `agent_thought_chunk` | 思考增量 |
| `tool_call` | 工具调用开始 |
| `tool_call_update` | 工具调用状态/结果更新 |
| `user_message_chunk` | 历史回放中的用户消息 |

复杂联合字段使用 `json.RawMessage` 保真，避免自研类型丢失规范字段。

### 7.4 权限请求

```json
{
  "sessionId": "...",
  "toolCall": {
    "toolCallId": "toolu_...",
    "title": "Edit src/main.dart",
    "kind": "edit",
    "status": "pending"
  },
  "options": [
    { "optionId": "allow",        "name": "Allow Once",   "kind": "allow_once" },
    { "optionId": "allow_always", "name": "Always Allow", "kind": "allow_always" },
    { "optionId": "reject",       "name": "Deny",         "kind": "reject_once" },
    { "optionId": "reject_always","name": "Always Deny",  "kind": "reject_always" }
  ]
}
```

响应：

```json
{ "outcome": { "outcome": "selected", "optionId": "allow" } }
```

或取消：

```json
{ "outcome": { "outcome": "cancelled" } }
```

### 7.5 错误映射

| 场景 | JSON-RPC 错误 |
|---|---|
| 未知方法 | `-32601 Method not found` |
| 参数非法 | `-32602 Invalid params` |
| 会话不存在 | `-32602`，message 说明 |
| CLI 未登录 / 认证失败 | `-32603`，附带 CLI stderr 摘要 |
| 进程异常退出 | `-32603`，附带退出码与 stderr 尾部 |
| 取消 | 请求返回 `cancelled` stopReason；`$/cancel_request` 对应请求返回 `-32800`（Request cancelled） |

---

## 8. Claude CLI 适配层

### 8.1 启动参数

已从 Claude Agent SDK 0.3.232 源码核实：

```bash
claude \
  --output-format stream-json \
  --verbose \
  --input-format stream-json \
  --include-partial-messages \
  --permission-prompt-tool stdio \
  --replay-user-messages \
  --session-id=<uuid>            # 新建会话
  --resume=<id>                  # session/load
  --model <model>
  --permission-mode <mode>
  --effort <level>
  --mcp-config <json>            # 客户端提供 MCP 时
  --setting-sources=user,project,local
  --add-dir <dir>                # additionalDirectories
```

环境：

- 继承父进程环境；
- 删除 `NODE_OPTIONS`（避免 CLI 子进程继承不兼容的 Node 参数）；
- `CLAUDE_CODE_ENTRYPOINT` 初始不设置；M0 实测是否需要（SDK 会设为 `sdk-ts` / `sdk-go`）。

### 8.2 编解码

- 读：`bufio.Reader.ReadBytes('\n')`，单行上限 32 MiB。
- 写：单 goroutine + channel，保证 stdin 不并发写。
- 每行一个 JSON 对象；畸形行记录日志并跳过（不 panic）。

### 8.3 CLI → agent 消息

| `type` | 用途 |
|---|---|
| `system` | `init`、`session_state_changed`、`commands_changed` |
| `assistant` | 完整助手消息，含 `tool_use` block |
| `user` | 工具结果（`tool_result` block），也可能含回显的用户消息 |
| `stream_event` | 增量：`text_delta` / `thinking_delta` / `input_json_delta` |
| `result` | 一轮结束、用量、错误 |
| `control_request` | CLI 反向请求（权限、hook 等） |
| `control_response` | 对 agent 控制请求的响应 |
| `keep_alive` | 忽略 |

`result` 映射：

| CLI | ACP stopReason |
|---|---|
| `subtype=success`, `is_error=false` | `end_turn` |
| `terminal_reason=max_turns` | `max_turn_requests` |
| `stop_reason=max_tokens` | `max_tokens` |
| `stop_reason=refusal` | `refusal` |
| 被 interrupt | `cancelled` |
| 其他错误 | 返回 JSON-RPC 错误 |

### 8.4 agent → CLI 控制请求

信封：

```json
{"type":"control_request","request_id":"<uuid>","request":{"subtype":"..."}}
```

请求：

| subtype | payload | 用途 |
|---|---|---|
| `initialize` | `{systemPrompt:{type:"preset",preset:"claude_code"}}` | 握手，返回 models / commands / agents |
| `interrupt` | `{}` | 取消当前 turn |
| `set_model` | `{model:"..."}` | 切换模型 |
| `set_permission_mode` | `{mode:"..."}` | 切换模式 |
| `apply_flag_settings` | `{settings:{effortLevel:"..."}}` | 运行时切换思考程度 |

响应：

```json
{"type":"control_response","response":{"subtype":"success","request_id":"<uuid>","response":{}}}
```

- 所有控制请求带超时（interrupt 30s，其余 10s）。
- `request_id` 使用随机 UUID；与响应按 id 关联。

### 8.5 CLI → agent 反向请求

| subtype | P1 处理 |
|---|---|
| `can_use_tool` | 转成 ACP `session/request_permission`；响应 `{behavior:"allow",updatedInput}` 或 `{behavior:"deny",message}` |
| `hook_callback` | 回错误（P1 不注册 hooks） |
| `elicitation` | 回 `{action:"decline"}` |
| `request_user_dialog` | 回错误 / 忽略 |
| 其他 | 回错误，记录日志 |

### 8.6 权限流时序

```
CLI ── control_request(can_use_tool) ──► agent
agent:
  1. 若 tool_use 尚未发过，先发一条 ACP tool_call（保证客户端能引用）
  2. conn.Call(session/request_permission) ──► client
  3. 等待客户端响应（可跨物理重连）
  4. 回 control_response:
       allow → {behavior:"allow", updatedInput}
       deny  → {behavior:"deny", message}
```

- 客户端取消 / turn 被 interrupt：回 `deny` 并带 `interrupt: true`。
- 客户端断线期间：请求保留在 Slot 的 pending 中，重连回放后继续等待。

### 8.7 消息翻译

| CLI | ACP |
|---|---|
| `stream_event` `text_delta` | `agent_message_chunk` |
| `stream_event` `thinking_delta` | `agent_thought_chunk` |
| `assistant` 的 `tool_use` | `tool_call` |
| `user` 的 `tool_result` | `tool_call_update` |
| `result` | 结束 `session/prompt` |

去重规则：

- 文本/思考只从 `stream_event` 增量发出；
- `assistant` 完整消息只用于提取 `tool_use` 和 usage；
- 若某条消息没有收到 partial 增量，回退到完整消息发一次。

工具 kind 映射：

| Claude 工具 | ACP kind |
|---|---|
| `Read` | `read` |
| `Edit` / `Write` / `NotebookEdit` | `edit` |
| `Bash` | `execute` |
| `Glob` / `Grep` | `search` |
| `WebFetch` / `WebSearch` | `fetch` |
| `Task` / `Agent` | `other` |
| 其他 | `other` |

### 8.8 模型 / 思考程度 / 模式

会话创建后：

- 模型列表：来自 `initialize` 控制请求响应的 `models` 字段；
- effort 档位：来自同一响应或 `supportedEffortLevels`；
- 模式（与 `claude-agent-acp` 的 `buildAvailableModes` 对齐）：
  - `auto`（名称 Auto）——仅当当前模型 `supportsAutoMode: true` 时提供；
  - `manual`（名称 **Manual**）——CLI 的标准权限模式；不再使用旧名 `default`；
  - `acceptEdits`、`plan`、`dontAsk`——始终提供；
  - `bypassPermissions`——仅当 `!root || IS_SANDBOX` 时提供；此时 CLI 以 `--allow-dangerously-skip-permissions` 启动。
  - 切换模型后重新计算模式列表；当前模式不再可用时钳制回 `manual` 并同步给 CLI。

转换为 ACP `configOptions`（与 npm adapter 的 id 保持一致）：

```json
[
  { "id": "mode",   "category": "mode",   "type": "select", "currentValue": "default", "options": [...] },
  { "id": "model",  "category": "model",  "type": "select", "currentValue": "default", "options": [...] },
  { "id": "effort", "category": "thought_level", "type": "select", "currentValue": "default", "options": [...] }
]
```

运行时切换：

| ACP | CLI |
|---|---|
| `set_config_option(model)` | `set_model` |
| `set_config_option(effort)` | `apply_flag_settings {effortLevel}` |
| `set_mode` | `set_permission_mode` |

### 8.9 会话历史

配置目录优先级：`CLAUDE_CONFIG_DIR` → `~/.claude`。

`session/list`：

- 扫描 `<config>/projects/*/*.jsonl`；
- 过滤条件：请求中的 `cwd`（读取 jsonl 头部/尾部记录的 `cwd`，规范化后比较）；
- 标题：summary 字段或首条用户消息；
- `updatedAt`：文件 mtime；
- `sessionId`：文件名（UUID）。

`session/load`：

- spawn 时传 `--resume=<id>`；
- 解析 jsonl，将用户/助手文本按顺序回放为 `session/update`（`user_message_chunk` / `agent_message_chunk`）；
- 工具调用与结果的回放放 P2。

> jsonl 是 CLI 内部格式：所有字段可选、解析失败降级为“无历史”，不 panic。

---

## 9. 并发、生命周期与故障

### 9.1 goroutine 模型

| goroutine | 数量 | 职责 |
|---|---|---|
| HTTP accept | 1 | `net/http` |
| WS 读 pump | 每物理连接 1 | 读帧 → 过滤/去重 → inbound |
| WS 写 pump | 每物理连接 1 | outbox → 写帧 |
| rpc 分发 | 每请求 1 | 异步 handler |
| CLI stdout 读 | 每会话 1 | stream-json → 翻译 |
| CLI stdin 写 | 每会话 1 | 请求队列 → stdin |
| Slot 回收 | 1 | 宽限期扫描 |

### 9.2 会话生命周期

```
session/new ──► spawn CLI ──► initialize 控制请求
session/prompt ──► 写 user 消息 ──► 读 result
session/cancel ──► control_request(interrupt) ──► prompt 返回 cancelled
session/close ──► 取消 turn ──► 关闭 stdin ──► 杀进程组 ──► 删除会话
```

- prompt 按会话串行：并发 prompt 返回错误。
- cancel 与 prompt 并发安全：cancel 只发控制请求，不改状态。
- 进程异常退出：进行中的 prompt 返回 `-32603`；会话标记 dead，`session/load` 可恢复。

### 9.3 优雅退出

1. 停止接受新连接；
2. 取消所有 turn；
3. 关闭 claude stdin，等待 5s；
4. kill 进程组；
5. 关闭 WS。

---

## 10. 配置

```text
--addr            监听地址，默认 127.0.0.1:3000
--token           Bearer token（env: CLAUDE_ACP_TOKEN）
--claude-path     claude 可执行文件（env: CLAUDE_CODE_EXECUTABLE，默认 "claude"）
--cwd-root        可选的 cwd 白名单根目录
--session-grace   断线会话保留时间，默认 10m
--outbox-max      outbox 上限，默认 8MiB
--max-sessions    每 Slot 会话上限，默认 16
--tls-cert        TLS 证书
--tls-key         TLS 私钥
--log-level       info | debug
```

启动检查：

- `claude --version` 成功；版本不在已验证区间时输出 warning；
- 非 loopback 绑定且无 token → 启动失败；
- `--cwd-root` 必须是绝对路径且存在。

---

## 11. 分期与验收

| 阶段 | 内容 | 验收标准 |
|---|---|---|
| **M0** 骨架（2–3 天） | go.mod、WS server、rpc 层、ACP initialize、fake CLI | `go test -race ./...` 绿；任意 WS 客户端能完成 `initialize` |
| **M1** omux 对齐（2–3 周） | Claude 适配、P1 全部方法、权限、重连/回放/去重 | ① `dart run tool/verify_acp.dart` 全流程通过；② omux 真机：聊天、工具审批、切模型/effort、断网重连不丢 turn；③ `go test -race` 绿 |
| **M2** 通用化 | resume/fork/delete、图片、slash commands、plan、subagent | 任意 ACP-over-WS 客户端可完成基本会话 |
| **M3** 扩展 | elicitation、providers、goal、steer | 按需 |

---

## 12. 测试策略

### 12.1 单元测试

- `internal/rpc`：并发调用、通知、超时、取消、字符串/数字 id、断连。
- `internal/ws`：Slot 复用、outbox 顺序、去重、宽限期、overflow、鉴权。
- `internal/agent`：消息翻译、工具 kind、stopReason、权限映射。
- `internal/claude`：参数拼装、控制请求关联、畸形行处理。

### 12.2 协议一致性

- vendor 官方 ACP `schema.json`；
- 对每个收发的 ACP 样本做 JSON Schema 校验（仅测试依赖，例如 `github.com/santhosh-tekuri/jsonschema`）；
- 可选：使用官方 ACP TCK（Python 兼容性测试套件）验证协议一致性。

### 12.3 Fake CLI

一个可编排的 Go 测试子进程：

- 按脚本输出 `system/init`、`stream_event`、`tool_use`、`can_use_tool`、`result`；
- 覆盖权限允许/拒绝、interrupt、进程崩溃、超大行、畸形 JSON；
- 用于 CI 中的确定性端到端测试。

### 12.4 黄金 transcript

用固定版本的真实 `claude` 录制对话（`--debug-file` 或直接抓 stdout），作为解码回归基线。

### 12.5 真实端到端

- `dart run tool/verify_acp.dart ws://127.0.0.1:3000 <cwd>`；
- omux 真机全流程；
- 网络中断模拟（关 WiFi / 代理断流）验证重连。

---

## 13. 风险与开放问题

| 风险 | 影响 | 应对 |
|---|---|---|
| Claude `stream-json` / control 协议非公开，随 CLI 版本漂移 | 高 | 锁定已验证 CLI 区间；黄金 transcript 回归；防御式解析；`--claude-path` 支持指定版本 |
| ACP schema 版本演进 | 中 | vendor 并锁定；升级时更新类型 + 回归 |
| 自研 JSON-RPC 边界情况 | 中 | 单元测试覆盖并发/取消/断连；样本 schema 校验 |
| jsonl 是 CLI 内部格式 | 中 | 只读、字段可选、失败降级 |
| `CLAUDE_CODE_ENTRYPOINT` 未设置的行为未知 | 低 | M0 实测，必要时设置 |
| `--replay-user-messages` 的真实语义 | 低 | M0 实测；无效则移除 |
| 单凭据模型 | 中 | P1 明确单用户；多租户另立设计 |
| 无 stdio | 低 | 按需求不做；未来如需，补一个 stdio transport |

### 待确认

1. GitHub owner：`github.com/coxlong/claude-acp` 是否合适？
2. License：Apache-2.0？
3. 部署目标：Linux 单二进制 + Dockerfile？

---

## 14. 参考与事实来源

- ACP 官方 SDK（无 Go）：`agentclientprotocol/typescript-sdk`、`rust-sdk`、`kotlin-sdk`、`python-sdk`、`java-sdk`
- ACP Go 社区库（**不采用**）：`coder/acp-go-sdk`
- Claude Agent SDK 发布包 `@anthropic-ai/claude-agent-sdk@0.3.232`（用于核实 CLI 参数与控制协议）
- Claude Code CLI：本机 `2.1.302`
- `github.com/gorilla/websocket` v1.5.3
