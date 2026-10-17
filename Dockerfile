# Build stage
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/claude-acp ./cmd/claude-acp

# Runtime stage: the Claude Code CLI is required on the server.
# Pin CLAUDE_CODE_VERSION to the version you have validated.
FROM node:22-bookworm-slim
ARG CLAUDE_CODE_VERSION=2.1.302
RUN npm install -g @anthropic-ai/claude-code@${CLAUDE_CODE_VERSION}
COPY --from=build /out/claude-acp /usr/local/bin/claude-acp

# The container runs as a user with Claude credentials mounted at ~/.claude.
# Example:
#   docker run --rm -p 3000:3000 \
#     -e CLAUDE_ACP_TOKEN=secret \
#     -v "$HOME/.claude:/home/node/.claude" \
#     claude-acp --addr 0.0.0.0:3000
USER node
EXPOSE 3000
ENTRYPOINT ["/usr/local/bin/claude-acp", "serve", "--addr", "0.0.0.0:3000"]
