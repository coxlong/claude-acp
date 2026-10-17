# claude-acp

A Go implementation of an [Agent Client Protocol](https://agentclientprotocol.com) (ACP) agent that drives the **Claude Code CLI** directly, over **WebSocket**.

- **ACP**: protocol version 1, NDJSON frames, JSON-RPC 2.0.
- **Backend**: the local `claude` CLI (no Claude Agent SDK, no Node adapter).
- **Transport**: WebSocket only. Logical clients survive reconnects; sessions keep running while disconnected.
- **Dependencies**: `gorilla/websocket` is the only runtime third-party dependency. The ACP and Claude CLI protocol layers are implemented in this repository.

See [DESIGN.md](DESIGN.md) for the architecture and protocol details.

## Status

P1 (omux-compatible surface) is implemented:

| Area | Methods |
|---|---|
| Session | `initialize`, `authenticate`, `session/new`, `session/load`, `session/list`, `session/close` |
| Turn | `session/prompt`, `session/cancel` |
| Config | `session/set_mode`, `session/set_config_option` (`model`, `effort`) |
| Client calls | `session/update`, `session/request_permission` |

Not yet implemented (returns `-32601` and is not advertised): `session/resume`, `session/fork`, `session/delete`, images, slash commands, elicitation, providers, goals, subagent transcripts.

## Quick start

```bash
go build -o claude-acp ./cmd/claude-acp
./claude-acp doctor                      # check the claude CLI and config dir
./claude-acp serve --addr 127.0.0.1:3000
```

Connect any ACP-over-WebSocket client to `ws://127.0.0.1:3000`. For a non-loopback bind a token is required:

```bash
CLAUDE_ACP_TOKEN=secret ./claude-acp serve --addr 0.0.0.0:3000
```

The client sends `Authorization: Bearer secret` on the WebSocket upgrade.

### WebSocket bridge semantics

- Every connection starts with a preamble:
  - first connect: `{"type":"connected","clientId":"<id>"}`
  - reconnect: `{"type":"reconnect","clientId":"<id>"}`
- Send `X-Client-Id` on reconnect to reuse the logical client (and its running sessions).
- One JSON-RPC object per WebSocket text message; the server also accepts multiple newline-delimited objects per frame.
- `$/ping` notifications are swallowed by the bridge.
- Output produced while disconnected is buffered (default 8 MiB) and replayed after the preamble.
- Duplicate request IDs are answered from a response cache instead of being executed twice.
- Sessions are kept for `--session-grace` (default 10 minutes) after a disconnect.

## Flags

```
--addr             listen address (default 127.0.0.1:3000)
--token            bearer token (env CLAUDE_ACP_TOKEN)
--claude-path      path to the claude CLI (env CLAUDE_CODE_EXECUTABLE)
--cwd-root         optional allowlist root for session cwd
--session-grace    disconnect grace period (default 10m)
--outbox-max       per-client outbox byte limit (default 8MiB)
--max-sessions     sessions per client (default 16)
--log-level        debug|info|warn|error
--tls-cert/--tls-key
--config-dir       Claude config directory (default ~/.claude)
```

## Claude CLI invocation

Each ACP session owns one long-lived CLI process:

```bash
claude --output-format stream-json --verbose \
       --input-format stream-json --include-partial-messages \
       --permission-prompt-tool stdio --replay-user-messages \
       --session-id=<uuid> | --resume=<id> \
       --model <m> --permission-mode <pm> --effort <level> \
       --mcp-config <json> --setting-sources=user,project,local
```

Permissions are bridged from the CLI `can_use_tool` control request to ACP
`session/request_permission`. Runtime controls use `set_model`,
`set_permission_mode`, and `apply_flag_settings` (`effortLevel`).

> The Claude stream-json/control protocol is not a public contract. Pin the CLI
> version you test against and capture golden transcripts before upgrading. The
> CLI version reported by `claude --version` is checked by `doctor`.

## Development

```bash
make fmt vet test     # gofmt, go vet, go test -race ./...
make build
```

Tests include:

- JSON-RPC correlation, notifications, cancellation (`internal/rpc`).
- WebSocket initialize, reconnect/replay, duplicate suppression, auth (`internal/ws`).
- A fake CLI (the test binary re-executed with `FAKE_CLI=1`) exercising
  streaming, permissions, and runtime control requests (`internal/claude`).
- Official ACP JSON Schema validation of emitted messages (`internal/acp`).

End-to-end against a real CLI:

```bash
./claude-acp serve --addr 127.0.0.1:3000 &
go run ./tool/e2e ws://127.0.0.1:3000 /tmp/some-project
```

## Security

- Non-loopback binds require a token; the process refuses to start otherwise.
- `--cwd-root` restricts which working directories sessions can use.
- Only the bearer token is checked; TLS is expected to be terminated by a
  reverse proxy (or configure `--tls-cert`/`--tls-key`).
- The agent never implements client-side `fs/*` or `terminal/*` methods.
- The server runs with the local user's Claude credentials; it is single-tenant
  by design.
